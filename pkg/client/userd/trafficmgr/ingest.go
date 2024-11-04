package trafficmgr

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
)

type ingest struct {
	*manager.AgentInfo
	ctx             context.Context
	cancel          context.CancelFunc
	containerName   string
	mountPoint      string
	localMountPoint string
	localMountPort  int32
	localPorts      []string
}

func (ig *ingest) podAccess(rd daemon.DaemonClient) *podAccess {
	ni := ig.Containers[ig.containerName]
	pa := &podAccess{
		ctx:              ig.ctx,
		localPorts:       ig.localPorts,
		container:        ig.containerName,
		podIP:            ig.PodIp,
		sftpPort:         ig.SftpPort,
		ftpPort:          ig.FtpPort,
		mountPoint:       ni.MountPoint,
		clientMountPoint: ig.localMountPoint,
	}
	if err := pa.ensureAccess(ig.ctx, rd); err != nil {
		dlog.Error(ig.ctx, err)
	}
	return pa
}

func (ig *ingest) response() *rpc.IngestResponse {
	cn := ig.Containers[ig.containerName]
	return &rpc.IngestResponse{
		PodIp:            ig.PodIp,
		SftpPort:         ig.SftpPort,
		FtpPort:          ig.FtpPort,
		ClientMountPoint: ig.localMountPoint,
		Environment:      cn.Environment,
	}
}

func (s *session) getSingleContainerName(ai *manager.AgentInfo) (name string, err error) {
	switch len(ai.Containers) {
	case 0:
		err = status.Error(codes.Unavailable, fmt.Sprintf("traffic-manager %s have no support for ingest", s.managerVersion))
	case 1:
		for name = range ai.Containers {
		}
	default:
		err = status.Error(codes.NotFound, fmt.Sprintf("workload %s has multiple containers. Please specify which one to use", ai.Name))
	}
	return name, err
}

func (s *session) Ingest(ctx context.Context, rq *rpc.IngestRequest) (ir *rpc.IngestResponse, err error) {
	var ai *manager.AgentInfo
	id := rq.Identifier
	if id.ContainerName == "" {
		ai, err = s.managerClient.EnsureAgent(ctx, &manager.EnsureAgentRequest{Session: s.sessionInfo, Name: id.WorkloadName})
		if err != nil {
			return nil, err
		}
		id.ContainerName, err = s.getSingleContainerName(ai)
		if err != nil {
			return nil, err
		}
	}

	ik := ingestKey{
		workload:  id.WorkloadName,
		container: id.ContainerName,
	}
	ig, _ := s.currentIngests.Compute(ik, func(oldValue *ingest, loaded bool) (ig *ingest, delete bool) {
		if loaded {
			return oldValue, false
		}
		if ai == nil {
			ai, err = s.managerClient.EnsureAgent(ctx, &manager.EnsureAgentRequest{Session: s.sessionInfo, Name: id.WorkloadName})
			if err != nil {
				return nil, true
			}
		}
		ci, ok := ai.Containers[id.ContainerName]
		if !ok {
			err = fmt.Errorf("workload %s has no container named %s", id.WorkloadName, id.ContainerName)
			return nil, true
		}
		ctx, cancel := context.WithCancel(ctx)
		cancelIngest := func() {
			s.currentIngests.Delete(ik)
			cancel()
		}
		ig = &ingest{
			AgentInfo:       ai,
			ctx:             ctx,
			cancel:          cancelIngest,
			containerName:   id.ContainerName,
			mountPoint:      ci.MountPoint,
			localMountPoint: rq.MountPoint,
			localMountPort:  rq.LocalMountPort,
			localPorts:      rq.LocalPorts,
		}
		return ig, false
	})
	if err != nil {
		return nil, err
	}

	if s.ingestLoopRunning.CompareAndSwap(false, true) {
		s.ingestTracker = newPodAccessTracker()
		go func() {
			if err := s.watchAgentsLoop(ctx); err != nil {
				dlog.Errorf(ctx, "failed to watch agents: %v", err)
				s.ingestLoopRunning.Store(false)
			}
		}()
	}
	s.ingestTracker.initialStart(ctx, ig.podAccess(s.rootDaemon))
	return ig.response(), nil
}

func (s *session) LeaveIngest(rq *rpc.IngestIdentifier) (err error) {
	ik := ingestKey{
		workload:  rq.WorkloadName,
		container: rq.ContainerName,
	}

	if rq.ContainerName == "" {
		// Valid if there's only one ingest for the given workload.
		s.currentIngests.Range(func(key ingestKey, value *ingest) bool {
			if key.workload == rq.WorkloadName {
				if rq.ContainerName != "" {
					err = status.Error(codes.NotFound, fmt.Sprintf("workload %s has multiple ingestions. Please specify which one to use", rq.WorkloadName))
					return false
				}
				rq.ContainerName = key.container
			}
			return true
		})
		if err != nil {
			return err
		}
	}
	if ig, ok := s.currentIngests.Load(ik); ok {
		ig.cancel()
		return nil
	}
	return status.Error(codes.NotFound, fmt.Sprintf("ingest %s[%s] doesn't exist", rq.WorkloadName, rq.ContainerName))
}

func (s *session) watchAgentsLoop(ctx context.Context) error {
	stream, err := s.managerClient.WatchAgents(ctx, s.SessionInfo())
	if err != nil {
		return fmt.Errorf("manager.WatchAgents: %w", err)
	}
	for ctx.Err() == nil {
		snapshot, err := stream.Recv()
		if err != nil {
			// Handle as if we had an empty snapshot. This will ensure that port forwards and volume mounts are canceled correctly.
			s.handleIngestSnapshot(ctx, nil)
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				// Normal termination
				return nil
			}
			return fmt.Errorf("manager.WatchAgents recv: %w", err)
		}
		s.handleIngestSnapshot(ctx, snapshot.Agents)
	}
	return nil
}

func (s *session) handleIngestSnapshot(ctx context.Context, infos []*manager.AgentInfo) {
	s.ingestTracker.initSnapshot()
	for _, info := range infos {
		for cn := range info.Containers {
			ik := ingestKey{
				workload:  info.Name,
				container: cn,
			}
			if ig, ok := s.currentIngests.Load(ik); ok {
				s.ingestTracker.start(ctx, ig.podAccess(s.rootDaemon))
			}
		}
	}
	s.ingestTracker.cancelUnwanted(ctx)
}
