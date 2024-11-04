package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/telepresenceio/telepresence/rpc/v2/connector"
)

func (s *notConnectedSuite) Test_Ingest() {
	ctx := s.Context()
	s.TelepresenceHelmInstallOK(ctx, true, "--set", "intercept.environment.excluded={DATABASE_HOST,DATABASE_PASSWORD}")
	defer s.RollbackTM(ctx)

	s.ApplyApp(ctx, "echo_with_env", "deploy/echo-easy")
	defer s.DeleteSvcAndWorkload(ctx, "deploy", "echo-easy")

	mountPoint := filepath.Join(s.T().TempDir(), "mnt")
	s.Require().NoError(os.Mkdir(mountPoint, 0o755))
	s.Require().NoError(s.withConnectedService(func(ctx context.Context, svc connector.ConnectorServer) {
		rsp, err := svc.Ingest(ctx, &connector.IngestRequest{
			MountPoint: mountPoint,
			Identifier: &connector.IngestIdentifier{
				WorkloadName: "echo-easy",
			},
		})
		rq := s.Require()
		rq.NoError(err)
		env := rsp.Environment
		s.Empty(env["DATABASE_HOST"])
		s.Empty(env["DATABASE_PASSWORD"])
		s.Equal("DATA", env["TEST"])
		s.Contains("ENV", env["INTERCEPT"])

		testDir := filepath.Join(rsp.ClientMountPoint, "var")
		s.Eventually(func() bool {
			st, err := os.Stat(testDir)
			return err == nil && st.Mode().IsDir()
		}, 15*time.Second, 3*time.Second)
	}))
}
