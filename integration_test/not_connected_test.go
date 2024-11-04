package integration_test

import (
	"bufio"
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	argorollouts "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned"
	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/k8sapi/pkg/k8sapi"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/integration_test/itest"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/connect"
	"github.com/telepresenceio/telepresence/v2/pkg/client/logging"
	"github.com/telepresenceio/telepresence/v2/pkg/client/portforward"
	"github.com/telepresenceio/telepresence/v2/pkg/client/userd"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
)

type notConnectedSuite struct {
	itest.Suite
	itest.NamespacePair
}

func (s *notConnectedSuite) SuiteName() string {
	return "NotConnected"
}

func init() {
	itest.AddTrafficManagerSuite("", func(h itest.NamespacePair) itest.TestingSuite {
		return &notConnectedSuite{Suite: itest.Suite{Harness: h}, NamespacePair: h}
	})
}

func (s *notConnectedSuite) TearDownTest() {
	itest.TelepresenceQuitOk(s.Context())
}

func (s *notConnectedSuite) Test_ConnectWithCommand() {
	ctx := s.Context()
	exe, _ := s.Executable()
	stdout := s.TelepresenceConnect(ctx, "--", exe, "status")
	s.Contains(stdout, "Connected to context")
	s.Contains(stdout, "Kubernetes context:")
}

func (s *notConnectedSuite) Test_InvalidKubeconfig() {
	ctx := s.Context()
	path := "/dev/null"
	if runtime.GOOS == "windows" {
		path = "C:\\NUL"
	}
	badEnvCtx := itest.WithEnv(ctx, map[string]string{"KUBECONFIG": path})
	_, stderr, err := itest.Telepresence(badEnvCtx, "connect")
	s.Contains(stderr, "kubeconfig has no context definition")
	s.Error(err)
}

func (s *notConnectedSuite) Test_NonExistentContext() {
	ctx := s.Context()
	_, stderr, err := itest.Telepresence(ctx, "connect", "--context", "not-likely-to-exist")
	s.Error(err)
	s.Contains(stderr, "context was not found")
}

func (s *notConnectedSuite) Test_ConnectingToOtherNamespace() {
	ctx := s.Context()

	suffix := itest.GetGlobalHarness(s.HarnessContext()).Suffix()
	appSpace2, mgrSpace2 := itest.AppAndMgrNSName(suffix + "-2")
	itest.CreateNamespaces(ctx, appSpace2, mgrSpace2)
	defer itest.DeleteNamespaces(ctx, appSpace2, mgrSpace2)

	s.Run("Installs Successfully", func() {
		ctx := itest.WithNamespaces(s.Context(), &itest.Namespaces{
			Namespace:         mgrSpace2,
			ManagedNamespaces: []string{appSpace2},
		})
		s.TelepresenceHelmInstallOK(ctx, false)
	})

	s.Run("Can be connected to with --manager-namespace-flag", func() {
		ctx := s.Context()
		itest.TelepresenceQuitOk(ctx)

		// Set the config to some nonsense to verify that the flag wins
		ctx = itest.WithConfig(ctx, func(cfg client.Config) {
			cfg.Cluster().DefaultManagerNamespace = "daffy-duck"
		})
		ctx = itest.WithUser(ctx, mgrSpace2+":"+itest.TestUser)
		stdout := itest.TelepresenceOk(ctx, "connect", "--namespace", appSpace2, "--manager-namespace="+mgrSpace2)
		s.Contains(stdout, "Connected to context")
		stdout = itest.TelepresenceOk(ctx, "status")
		s.Regexp(`Manager namespace\s+: `+mgrSpace2, stdout)
	})

	s.Run("Can be connected to with defaultManagerNamespace config", func() {
		ctx := s.Context()
		itest.TelepresenceQuitOk(ctx)
		ctx = itest.WithConfig(ctx, func(cfg client.Config) {
			cfg.Cluster().DefaultManagerNamespace = mgrSpace2
		})
		stdout := itest.TelepresenceOk(itest.WithUser(ctx, "default"), "connect")
		s.Contains(stdout, "Connected to context")
		stdout = itest.TelepresenceOk(ctx, "status")
		s.Regexp(`Manager namespace\s+: `+mgrSpace2, stdout)
	})

	s.Run("Uninstalls Successfully", func() {
		s.UninstallTrafficManager(s.Context(), mgrSpace2)
	})
}

func (s *notConnectedSuite) Test_ReportsNotConnected() {
	ctx := s.Context()
	itest.TelepresenceOk(itest.WithUser(ctx, "default"), "connect")
	itest.TelepresenceDisconnectOk(ctx)
	stdout := itest.TelepresenceOk(ctx, "version")
	rxVer := regexp.QuoteMeta(s.TelepresenceVersion())
	s.Regexp(fmt.Sprintf(`Client\s*: %s`, rxVer), stdout)
	s.Regexp(fmt.Sprintf(`Root Daemon\s*: %s`, rxVer), stdout)
	s.Regexp(fmt.Sprintf(`User Daemon\s*: %s`, rxVer), stdout)
	s.Regexp(`Traffic Manager\s*: not connected`, stdout)
}

// Test_CreateAndRunIndividualPod tess that pods can be created without a workload.
func (s *notConnectedSuite) Test_CreateAndRunIndividualPod() {
	out := s.KubectlOk(s.Context(), "run", "-i", "busybox", "--rm", "--image", "curlimages/curl", "--restart", "Never", "--", "ls", "/etc")
	lines := bufio.NewScanner(strings.NewReader(out))
	hostsFound := false
	for lines.Scan() {
		if lines.Text() == "hosts" {
			hostsFound = true
			break
		}
	}
	s.True(hostsFound, "remote ls command did not find /etc/hosts")
}

func (s *notConnectedSuite) trafficManagerConnection(ctx context.Context) (*grpc.ClientConn, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", itest.KubeConfig(ctx))
	if err != nil {
		return nil, err
	}
	return dialTrafficManager(ctx, cfg, s.ManagerNamespace())
}

func dialTrafficManager(ctx context.Context, cfg *rest.Config, managerNamespace string) (*grpc.ClientConn, error) {
	k8sApi, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	argoRollouApi, err := argorollouts.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	ctx = k8sapi.WithJoinedClientSetInterface(ctx, k8sApi, argoRollouApi)
	ctx = portforward.WithRestConfig(ctx, cfg)
	return grpc.NewClient(fmt.Sprintf(portforward.K8sPFScheme+":///svc/traffic-manager.%s:8081", managerNamespace),
		grpc.WithResolvers(portforward.NewResolver(ctx)),
		grpc.WithContextDialer(portforward.Dialer(ctx)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func (s *notConnectedSuite) doConnected(f func(context.Context, context.CancelFunc, manager.ManagerClient, *manager.SessionInfo)) {
	rq := s.Require()
	ctx := s.Context()
	conn, err := s.trafficManagerConnection(ctx)
	rq.NoError(err)
	defer conn.Close()

	client := manager.NewManagerClient(conn)

	// Retrieve the session info from the traffic-manager. This is how
	// a connection to a namespace is made. The traffic-manager now
	// associates the returned session with that namespace in subsequent
	// calls.
	clientSession, err := client.ArriveAsClient(ctx, &manager.ClientInfo{
		Name:      "telepresence@datawire.io",
		Namespace: s.AppNamespace(),
		InstallId: "xxx",
		Product:   "telepresence",
		Version:   s.TelepresenceVersion(),
	})
	rq.NoError(err)

	// Normal ticker routine to keep the client alive.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _ = client.Remain(ctx, &manager.RemainRequest{Session: clientSession})
			case <-ctx.Done():
				_, _ = client.Depart(ctx, clientSession)
				return
			}
		}
	}()
	f(ctx, cancel, client, clientSession)
}

func (s *notConnectedSuite) Test_DirectConnect() {
	s.Require().NoError(s.withConnectedService(func(ctx context.Context, server rpc.ConnectorServer) {
		v, err := server.Version(ctx, nil)
		s.Require().NoError(err)
		dlog.Info(ctx, v.Name, v.Version)
	}))
}

func (s *notConnectedSuite) withConnectedService(f func(context.Context, rpc.ConnectorServer)) error {
	client.ProcessName = func() string {
		return userd.ProcessName
	}
	ctx := cli.InitContext(s.Context())
	ctx, err := logging.InitContext(ctx, "connector", logging.RotateNever, true, true)
	if err != nil {
		return err
	}

	ctx = dos.WithExe(ctx, s)
	ctx = itest.WithConfig(ctx, func(cfg client.Config) {
		cfg.Intercept().UseFtp = false
	})
	err = connect.EnsureRootDaemonRunning(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := dgroup.NewGroup(ctx, dgroup.GroupConfig{
		SoftShutdownTimeout:  2 * time.Second,
		EnableSignalHandling: true,
		ShutdownOnNonError:   true,
	})
	srv, err := userd.GetNewServiceFunc(ctx)(ctx, g, client.GetConfig(ctx), grpc.NewServer())
	if err != nil {
		return err
	}
	g.Go("connector", srv.ManageSessions)

	var sv rpc.ConnectorServer
	srv.As(&sv)
	var rsp *rpc.ConnectInfo
	flags := map[string]string{
		"kubeconfig": itest.KubeConfig(ctx),
		"namespace":  s.AppNamespace(),
	}
	if user := itest.GetUser(ctx); user != "default" {
		flags["as"] = "system:serviceaccount:" + user
	}
	rsp, err = sv.Connect(ctx, &rpc.ConnectRequest{
		KubeFlags:        flags,
		MappedNamespaces: []string{s.AppNamespace()},
		ManagerNamespace: s.ManagerNamespace(),
		Environment:      s.GlobalEnv(ctx),
		SubnetViaWorkloads: []*daemon.SubnetViaWorkload{
			{
				Subnet:   "also",
				Workload: "echo-easy",
			},
			{
				Subnet:   "service",
				Workload: "echo-easy",
			},
			{
				Subnet:   "pods",
				Workload: "echo-easy",
			},
		},
	})
	if err != nil {
		return err
	}
	if rsp.Error != rpc.ConnectInfo_UNSPECIFIED && rsp.Error != rpc.ConnectInfo_ALREADY_CONNECTED {
		return errcat.Category(rsp.ErrorCategory).New(rsp.ErrorText)
	}
	func() {
		defer sv.Quit(ctx, nil)
		f(ctx, sv)
	}()
	return g.Wait()
}
