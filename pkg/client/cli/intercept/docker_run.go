package intercept

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/datawire/dlib/dcontext"
	"github.com/datawire/dlib/dexec"
	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/flags"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/output"
	"github.com/telepresenceio/telepresence/v2/pkg/client/docker"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/proc"
)

func (s *state) prepareDockerRun(ctx context.Context) error {
	imageName, _ := firstDockerArg(s.Cmdline)
	// Ensure that the image is ready to run before we create the intercept.
	return docker.PullImage(ctx, imageName)
}

var dockerBoolFlags = map[string]bool{ //nolint:gochecknoglobals // this is a constant
	"--detach":           true,
	"--init":             true,
	"--interactive":      true,
	"--no-healthcheck":   true,
	"--oom-kill-disable": true,
	"--privileged":       true,
	"--publish-all":      true,
	"--quiet":            true,
	"--read-only":        true,
	"--rm":               true,
	"--sig-proxy":        true,
	"--tty":              true,
}

// firstDockerArg returns the first argument that isn't an option. This requires knowledge
// about boolean docker flags, and if new such flags arrive and are used, this
// function might return an incorrect image.
func firstDockerArg(args []string) (string, int) {
	t := len(args)
	for i := 0; i < t; i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			return arg, i
		}
		if strings.IndexByte(arg, '=') > 0 {
			continue
		}
		if strings.HasPrefix(arg, "--") {
			if !dockerBoolFlags[arg] {
				i++
			}
		} else if strings.ContainsAny(arg, "ehlmpuvw") {
			// Shorthand flag that require an argument. Might be prefixed by shorthand booleans, e.g. -itl <label>
			i++
		}
	}
	return "", -1
}

type dockerRun struct {
	cmd     *dexec.Cmd
	err     error
	name    string
	volumes []string
}

func (dr *dockerRun) wait(ctx context.Context) error {
	if len(dr.volumes) > 0 {
		defer func() {
			ctx, cancel := context.WithTimeout(dcontext.WithoutCancel(ctx), 2*time.Second)
			docker.StopVolumeMounts(ctx, dr.volumes)
			cancel()
		}()
	}

	if dr.err != nil {
		return errcat.NoDaemonLogs.New(dr.err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, proc.SignalsToForward...)
	defer func() {
		signal.Stop(sigCh)
	}()

	killTimer := time.AfterFunc(math.MaxInt64, func() {
		_ = dr.cmd.Process.Kill()
	})
	defer killTimer.Stop()

	go func() {
		select {
		case <-ctx.Done():
		case <-sigCh:
			close(sigCh)
		}
		// Kill the docker run after a grace period in case it isn't stopped
		killTimer.Reset(2 * time.Second)
		ctx, cancel := context.WithTimeout(dcontext.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := docker.StopContainer(ctx, dr.name); err != nil {
			dlog.Error(ctx, err)
		}
	}()

	// Errors caused by context or signal termination doesn't count.
	err := dr.cmd.Wait()
	select {
	case <-ctx.Done():
		err = nil
	case <-sigCh:
		err = nil
	default:
		err = errcat.NoDaemonLogs.New(err)
	}
	return err
}

func (s *state) startInDocker(ctx context.Context, daemonName, envFile string, args []string) *dockerRun {
	ourArgs := []string{
		"run",
		"--env-file", envFile,
	}
	dr := &dockerRun{}
	dr.name, dr.err = flags.GetUnparsedValue(args, "--name")
	if dr.err != nil {
		return dr
	}
	if dr.name == "" {
		dr.name = fmt.Sprintf("intercept-%s-%d", s.Name(), s.localPort)
		ourArgs = append(ourArgs, "--name", dr.name)
	}

	if daemonName == "" {
		ourArgs = append(ourArgs, "--dns-search", "tel2-search")
		if s.dockerPort != 0 {
			ourArgs = append(ourArgs, "-p", fmt.Sprintf("%d:%d", s.localPort, s.dockerPort))
		}
		dockerMount := ""
		if s.mountPoint != "" { // do we have a mount point at all?
			if dockerMount = s.DockerMount; dockerMount == "" {
				dockerMount = s.mountPoint
			}
		}
		if dockerMount != "" {
			ourArgs = append(ourArgs, "-v", fmt.Sprintf("%s:%s", s.mountPoint, dockerMount))
		}
	} else {
		ourArgs = append(ourArgs, "--network", "container:"+daemonName)

		// "--rm" is mandatory when using --docker-run against a docker daemon, because without it, the volumes
		// cannot be removed.
		_, set, err := flags.GetUnparsedBoolean(args, "--rm")
		if err != nil {
			dr.err = err
			return dr
		}
		if !set {
			ourArgs = append(ourArgs, "--rm")
		}
		if !s.mountDisabled {
			m := s.info.Mount
			if m != nil {
				if err := docker.EnsureVolumePlugin(ctx); err != nil {
					fmt.Fprintf(output.Err(ctx), "Remote mount disabled: %s\n", err)
				}
				container := s.env["TELEPRESENCE_CONTAINER"]
				dlog.Infof(ctx, "Mounting %v from container %s", m.Mounts, container)
				dr.volumes, ourArgs, dr.err = docker.StartVolumeMounts(ctx, daemonName, container, m.Port, m.Mounts, nil, ourArgs)
				if dr.err != nil {
					return dr
				}
			}
		}
	}

	args = append(ourArgs, args...)
	dr.cmd, dr.err = proc.Start(dcontext.WithoutCancel(ctx), nil, "docker", args...)
	return dr
}
