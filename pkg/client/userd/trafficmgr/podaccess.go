package trafficmgr

import (
	"context"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/remotefs"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
)

type podAccess struct {
	// ctx is a context cancelled by the cancel attribute. It must be used by
	// services that should be cancelled when the intercept ends
	ctx context.Context

	// cancel is called when the intercept is no longer present
	cancel context.CancelFunc

	// wg is the group to wait for after a call to cancel
	wg sync.WaitGroup

	localPorts       []string
	container        string
	podIP            string
	sftpPort         int32
	ftpPort          int32
	mountPoint       string
	clientMountPoint string

	// The mounter of the remote file system.
	remotefs.Mounter

	// Use bridged ftp/sftp mount through this local port
	localMountPort int32
}

// podAccessKey identifies an intercepted pod. Although an intercept may span multiple
// pods, the user daemon will always choose exactly one pod with an active intercept to
// do port forwards and remote mounts.
type podAccessKey struct {
	container string
	podIP     string
}

// The podAccessSync provides pod specific synchronization for cancellation of port forwards
// and mounts. Cancellation here does not mean that the ingest/intercept is cancelled. It just
// means that the given pod is no longer the chosen one. This typically happens when pods
// are scaled down and then up again.
type podAccessSync struct {
	wg        sync.WaitGroup
	cancelPod context.CancelFunc
}

// podAccessTracker is what the traffic-manager is using to keep track of the chosen pods for
// the currently active intercepts.
type podAccessTracker struct {
	sync.Mutex

	// alive contains a map of the currently tracked podAccessSync
	alivePods map[podAccessKey]*podAccessSync

	// A snapshot is recreated for each new intercept snapshot read from the manager.
	// The set controls which podAccessTracker that are considered alive when cancelUnwanted
	// is called
	snapshot map[podAccessKey]struct{}

	// mountsReady contains channels that are closed when the mounts are prepared
	mountsReady map[podAccessKey]chan struct{}
}

func (pa *podAccess) shouldForward() bool {
	return len(pa.localPorts) > 0
}

// startForwards starts port forwards and mounts for the given podAccessKey.
// It assumes that the user has called shouldForward and is sure that something will be started.
func (pa *podAccess) startForwards(ctx context.Context, wg *sync.WaitGroup) {
	for _, port := range pa.localPorts {
		var pfCtx context.Context
		if iputil.IsIpV6Addr(pa.podIP) {
			pfCtx = dgroup.WithGoroutineName(ctx, fmt.Sprintf("/[%s]:%s", pa.podIP, port))
		} else {
			pfCtx = dgroup.WithGoroutineName(ctx, fmt.Sprintf("/%s:%s", pa.podIP, port))
		}
		wg.Add(1)
		go pa.workerPortForward(pfCtx, port, wg)
	}
}

func (pa *podAccess) ensureAccess(ctx context.Context, rd daemon.DaemonClient) error {
	if client.GetConfig(ctx).Cluster().AgentPortForward {
		// An agent port-forward to the pod with a designated to the podIP is necessary to
		// mount or port-forward to localhost.
		dlog.Debugf(ctx, "Waiting for root-daemon to receive agent IP %s", pa.podIP)
		rsp, err := rd.WaitForAgentIP(ctx, &daemon.WaitForAgentIPRequest{
			Ip:      iputil.Parse(pa.podIP),
			Timeout: durationpb.New(10 * time.Second),
		})
		switch status.Code(err) {
		case codes.Unavailable: // Unavailable means that the feature disabled. This is OK, the traffic-manager will do the forwarding
		case codes.OK:
			if lip, ok := netip.AddrFromSlice(rsp.LocalIp); ok {
				pa.podIP = lip.String()
			}
		case codes.DeadlineExceeded:
			return fmt.Errorf("timeout waiting for port-forward to traffic-agent with pod-ip %s", pa.podIP)
		default:
			return fmt.Errorf("unexpected error for port-forward to traffic-agent with pod-ip %s: %v", pa.podIP, err)
		}
	}
	return nil
}

func (pa *podAccess) workerPortForward(ctx context.Context, port string, wg *sync.WaitGroup) {
	defer wg.Done()
	pp, err := agentconfig.NewPortAndProto(port)
	if err != nil {
		dlog.Errorf(ctx, "malformed extra port %q: %v", port, err)
		return
	}
	addr, err := pp.Addr()
	if err != nil {
		dlog.Errorf(ctx, "unable to resolve extra port %q: %v", port, err)
		return
	}
	f := forwarder.NewInterceptor(addr, pa.podIP, pp.Port)
	err = f.Serve(ctx, nil)
	if err != nil && ctx.Err() == nil {
		dlog.Errorf(ctx, "port-forwarder failed with %v", err)
	}
}

func newPodAccessTracker() *podAccessTracker {
	return &podAccessTracker{alivePods: make(map[podAccessKey]*podAccessSync)}
}

// start a port forward for the given ingest/intercept and remembers that it's alive.
func (lpf *podAccessTracker) start(ctx context.Context, pa *podAccess) {
	// The mounts performed here are synced on by podIP + port to keep track of active
	// mounts. This is not enough in situations when a pod is deleted and another pod
	// takes over. That is two different IPs so an additional synchronization on the actual
	// mount point is necessary to prevent that it is established and deleted at the same
	// time.
	lpf.Lock()
	fk := podAccessKey{
		container: pa.container,
		podIP:     pa.podIP,
	}

	defer func() {
		if md, ok := lpf.mountsReady[fk]; ok {
			delete(lpf.mountsReady, fk)
			close(md)
		}
	}()

	// Make part of current snapshot tracking so that it isn't removed once the
	// snapshot has been completely handled
	lpf.snapshot[fk] = struct{}{}
	lpf.privateStartMounts(ctx, pa)
	lpf.Unlock()
}

func (lpf *podAccessTracker) initialStart(ctx context.Context, ic *podAccess) {
	lpf.Lock()
	lpf.privateStartMounts(ctx, ic)
	lpf.Unlock()
}

func (lpf *podAccessTracker) privateStartMounts(ctx context.Context, pa *podAccess) {
	if !pa.shouldForward() && !pa.shouldMount() {
		dlog.Debugf(ctx, "No mounts or port-forwards needed for pod-ip %s, container %s", pa.podIP, pa.container)
		return
	}

	// Already started?
	fk := podAccessKey{
		container: pa.container,
		podIP:     pa.podIP,
	}
	if _, isLive := lpf.alivePods[fk]; isLive {
		dlog.Debugf(ctx, "Mounts and port-forwards already active for %+v", fk)
		return
	}

	ctx, cancel := context.WithCancel(pa.ctx)
	lp := &podAccessSync{cancelPod: cancel}
	if pa.shouldMount() {
		pa.startMount(ctx, &pa.wg, &lp.wg)
	}
	if pa.shouldForward() {
		pa.startForwards(ctx, &lp.wg)
	}
	lpf.alivePods[fk] = lp
	dlog.Debugf(ctx, "Started mounts and port-forwards for pod-ip %s, container %s", pa.podIP, pa.container)
	return
}

// initSnapshot prepares this instance for a new round of start calls followed by a cancelUnwanted.
func (lpf *podAccessTracker) initSnapshot() {
	lpf.Lock()
	lpf.snapshot = make(map[podAccessKey]struct{})
	lpf.mountsReady = make(map[podAccessKey]chan struct{})
	lpf.Unlock()
}

func (lpf *podAccessTracker) getOrCreateMountsDone(pa *podAccess) <-chan struct{} {
	fk := podAccessKey{
		container: pa.container,
		podIP:     pa.podIP,
	}
	lpf.Lock()
	md, ok := lpf.mountsReady[fk]
	if !ok {
		md = make(chan struct{})
		lpf.mountsReady[fk] = md
	}
	lpf.Unlock()
	return md
}

// cancelUnwanted cancels all port forwards that hasn't been started since initSnapshot.
func (lpf *podAccessTracker) cancelUnwanted(ctx context.Context) {
	lpf.Lock()
	for fk, lp := range lpf.alivePods {
		if _, isWanted := lpf.snapshot[fk]; !isWanted {
			delete(lpf.alivePods, fk)
			md, ok := lpf.mountsReady[fk]
			if ok {
				delete(lpf.mountsReady, fk)
				close(md)
			}
			lpf.Unlock()
			dlog.Infof(ctx, "Terminating mounts and port-forwards for %+v", fk)
			lp.cancelPod()
			lp.wg.Wait()
			lpf.Lock()
		}
	}
	lpf.Unlock()
}
