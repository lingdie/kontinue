package hydrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"strconv"
	"strings"
	"time"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"golang.org/x/sync/singleflight"
)

type GVisorDriverConfig struct {
	RunscWrapperBin string
	RunscStateRoot  string
	BundlesDir      string
	BaseRootfsCache string
	CtrBinary       string
	UseHostNsenter  bool
	VolumeRootDir   string
}

// GVisorSandboxDriver runs standalone single-root gVisor sandboxes via Rust `kontinue-runsc`
// attached directly to each pre-warmed Carrier Pod's CNI network namespace (`--network=sandbox`).
type GVisorSandboxDriver struct {
	cfg         GVisorDriverConfig
	rootfsLoads singleflight.Group
}

func NewGVisorSandboxDriver(cfg GVisorDriverConfig) *GVisorSandboxDriver {
	if cfg.RunscWrapperBin == "" {
		cfg.RunscWrapperBin = "/usr/local/bin/kontinue-runsc"
	}
	if cfg.RunscStateRoot == "" {
		cfg.RunscStateRoot = "/run/kontinue/runsc"
	}
	if cfg.BundlesDir == "" {
		cfg.BundlesDir = "/var/lib/kontinue-arena/bundles"
	}
	if cfg.BaseRootfsCache == "" {
		cfg.BaseRootfsCache = "/var/lib/kontinue-arena/base-rootfs"
	}
	if cfg.CtrBinary == "" {
		cfg.CtrBinary = "ctr"
	}
	if cfg.VolumeRootDir == "" {
		cfg.VolumeRootDir = "/var/lib/kontinue-volumes"
	}
	if os.Getenv("KONTINUE_HOST_NSENTER") == "true" {
		cfg.UseHostNsenter = true
	}
	_ = os.MkdirAll(cfg.RunscStateRoot, 0o700)
	_ = os.MkdirAll(cfg.BundlesDir, 0o700)
	_ = os.MkdirAll(cfg.BaseRootfsCache, 0o700)
	_ = os.MkdirAll(cfg.VolumeRootDir, 0o700)
	return &GVisorSandboxDriver{cfg: cfg}
}

func (d *GVisorSandboxDriver) runHostCmdWithFileLog(ctx context.Context, logPath, bin string, args ...string) error {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return err
	}
	defer devNull.Close()

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	cmd := d.hostCommand(ctx, bin, args...)
	// CRITICAL: Use *os.File (NOT an os.Pipe / CombinedOutput!) so Go's cmd.Run() returns immediately
	// upon `runsc create/start/restore` exit without blocking on background `runsc-sandbox` daemons
	// that inherit container stdio file descriptors!
	cmd.Stdin = devNull
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	runErr := cmd.Run()
	_ = logFile.Close()
	if runErr != nil {
		out, _ := os.ReadFile(logPath)
		return fmt.Errorf("%w (%s)", runErr, string(out))
	}
	return nil
}

func (d *GVisorSandboxDriver) hostCommand(ctx context.Context, bin string, args ...string) *exec.Cmd {
	if !d.cfg.UseHostNsenter {
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Env = telemetry.InjectEnv(ctx, os.Environ())
		return cmd
	}
	var envArgs []string
	for _, kv := range telemetry.InjectEnv(ctx, nil) {
		envArgs = append(envArgs, kv)
	}
	nsArgs := []string{"-t", "1", "-m", "--"}
	if len(envArgs) > 0 {
		nsArgs = append(nsArgs, "env")
		nsArgs = append(nsArgs, envArgs...)
	}
	nsArgs = append(nsArgs, bin)
	nsArgs = append(nsArgs, args...)
	return exec.CommandContext(ctx, "nsenter", nsArgs...)
}

func (d *GVisorSandboxDriver) SwapAndStartSandboxContainer(ctx context.Context, opts SwapContainerOptions) (string, string, error) {
	rootfsDir, err := d.ensureBaseRootfs(ctx, opts.BaseImage)
	if err != nil {
		return "", "", fmt.Errorf("prepare base rootfs for %s: %w", opts.BaseImage, err)
	}

	// Each attempt owns its own bundle and cgroup, even if a slot is reused.
	cid := opts.ContainerID
	if cid == "" || filepath.Base(cid) != cid || !strings.HasPrefix(cid, "kt-") {
		return "", "", fmt.Errorf("invalid runsc instance ID")
	}
	bundleDir := filepath.Join(d.cfg.BundlesDir, cid)
	if err := os.Mkdir(bundleDir, 0o700); err != nil {
		return "", "", err
	}
	if opts.RestoreMode == protocol.RestoreModeFilesystemOnly && opts.RootfsOverlayDir != "" {
		isolatedRootfs := filepath.Join(bundleDir, "rootfs")
		if err := os.MkdirAll(isolatedRootfs, 0o755); err != nil {
			return "", "", err
		}
		if err := d.copyRootfs(ctx, rootfsDir, isolatedRootfs); err != nil {
			return "", "", err
		}
		if err := d.copyRootfs(ctx, opts.RootfsOverlayDir, isolatedRootfs); err != nil {
			return "", "", err
		}
		rootfsDir = isolatedRootfs
	}

	// The wrapper strips control-plane annotations before runsc compares OCI
	// specs during restore. Keep ownership outside the checkpointed spec.
	instance := bundleIdentity{ContainerID: cid, SlotID: opts.Slot.SlotID, PodUID: opts.Slot.PodUID,
		SandboxID: opts.SandboxID, ExecutionID: opts.ExecutionID, LifecycleID: opts.LifecycleID, ViewPath: opts.RestoreViewDir}
	rawOwner, err := json.Marshal(instance)
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(filepath.Join(bundleDir, "owner.json"), rawOwner, 0600); err != nil {
		return "", "", err
	}

	annotations := telemetry.InjectAnnotations(ctx, map[string]string{
		protocol.AnnotationSandboxID: opts.SandboxID,
	})
	if opts.RestoreViewDir != "" && opts.RestoreMode == protocol.RestoreModeMemory {
		annotations[protocol.GVisorAnnotationRestoreHostPath] = opts.RestoreViewDir
		annotations[protocol.GVisorAnnotationRestoreDirect] = "false"
	}
	mounts := []map[string]any{
		{"destination": "/proc", "type": "proc", "source": "proc"},
		{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "strictatime", "mode=755"}},
		{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
		{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "nodev", "mode=1777"}},
	}
	volumeRoot := opts.VolumeRootDir
	if volumeRoot == "" {
		volumeRoot = d.cfg.VolumeRootDir
	}
	for _, volume := range opts.Volumes {
		if volume.VolumeID == "" || filepath.Base(volume.VolumeID) != volume.VolumeID || volume.MountPath == "" || !filepath.IsAbs(volume.MountPath) {
			return "", "", fmt.Errorf("invalid volume mount")
		}
		hostPath := filepath.Join(volumeRoot, volume.VolumeID)
		if err := os.MkdirAll(hostPath, 0o700); err != nil {
			return "", "", err
		}
		options := []string{"rbind", "nosuid", "nodev"}
		if volume.ReadOnly {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}
		mounts = append(mounts, map[string]any{"destination": volume.MountPath, "type": "bind", "source": hostPath, "options": options})
	}

	namespaces := []map[string]string{
		{"type": "pid"},
		{"type": "ipc"},
		{"type": "uts"},
		{"type": "mount"},
	}
	if opts.Slot.NetNSPath != "" {
		namespaces = append(namespaces, map[string]string{
			"type": "network",
			"path": opts.Slot.NetNSPath,
		})
	} else {
		namespaces = append(namespaces, map[string]string{"type": "network"})
	}

	spec := map[string]any{
		"ociVersion": "1.0.2",
		"root": map[string]any{
			"path":     rootfsDir,
			"readonly": false,
		},
		// Keep OCI spec fields (hostname, args, env) invariant across checkpoint and restore
		// to satisfy gVisor's validateRestoreSpec; dynamic SandboxID, Hostname, AccessToken,
		// and EnvVars are injected immediately after boot/restore via ktd HeldInit!
		"hostname": "kontinue-sandbox",
		"process": map[string]any{
			"terminal": false,
			"user":     map[string]int{"uid": 0, "gid": 0},
			"cwd":      "/root",
			"args": []string{
				"/usr/local/bin/ktd",
				"--listen", fmt.Sprintf("0.0.0.0:%d", protocol.DefaultKtdPort),
			},
			"env": []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
				"HOME=/root",
				// Not on argv: /proc/1/cmdline is world-readable. Environ is mode 0400.
				// Value is invariant across checkpoint and restore for this sandbox.
				fmt.Sprintf("KTD_BOOTSTRAP_TOKEN=%s", opts.BootstrapToken),
			},
			"capabilities": map[string]any{
				"bounding":    defaultCaps(),
				"effective":   defaultCaps(),
				"inheritable": defaultCaps(),
				"permitted":   defaultCaps(),
				"ambient":     defaultCaps(),
			},
		},
		"mounts": mounts,
		"linux": map[string]any{
			"namespaces": namespaces,
		},
		"annotations": annotations,
	}

	rawSpec, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(filepath.Join(bundleDir, "config.json"), rawSpec, 0o600); err != nil {
		return "", "", err
	}

	if err := d.runHostCmdWithFileLog(ctx, filepath.Join(bundleDir, "create.log"), d.cfg.RunscWrapperBin,
		"--root="+d.cfg.RunscStateRoot,
		"create", "--bundle", bundleDir, cid,
	); err != nil {
		return "", "", fmt.Errorf("kontinue-runsc create %s failed: %w", cid, err)
	}

	if err := d.runHostCmdWithFileLog(ctx, filepath.Join(bundleDir, "start.log"), d.cfg.RunscWrapperBin,
		"--root="+d.cfg.RunscStateRoot,
		"start", cid,
	); err != nil {
		_ = d.StopSandboxContainer(context.Background(), opts.Slot, cid)
		return "", "", fmt.Errorf("kontinue-runsc start/restore %s failed: %w", cid, err)
	}
	if err := d.applyHostQuota(ctx, cid, opts.CPUMillis, opts.MemoryBytes); err != nil {
		_ = d.StopSandboxContainer(context.Background(), opts.Slot, cid)
		return "", "", err
	}

	return cid, fmt.Sprintf("%s:%d", opts.Slot.PodIP, protocol.DefaultKtdPort), nil
}

func (d *GVisorSandboxDriver) copyRootfs(ctx context.Context, source, target string) error {
	cmd := d.hostCommand(ctx, "cp", "-a", source+"/.", target+"/")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("copy rootfs %s into %s: %w (%s)", source, target, err, output)
	}
	return nil
}

func (d *GVisorSandboxDriver) CheckpointSandboxContainer(ctx context.Context, containerID, rawOutputDir string, leaveRunning bool) error {
	if err := os.MkdirAll(rawOutputDir, 0o700); err != nil {
		return err
	}
	args := []string{
		"--root=" + d.cfg.RunscStateRoot,
		"checkpoint",
		"--compression=none",
		"--image-path=" + rawOutputDir,
	}
	if leaveRunning {
		args = append(args, "--leave-running")
	}
	args = append(args, containerID)

	ckptLog := filepath.Join(filepath.Dir(rawOutputDir), "ckpt-"+containerID+".log")
	defer func() { _ = os.Remove(ckptLog) }()
	if err := d.runHostCmdWithFileLog(ctx, ckptLog, d.cfg.RunscWrapperBin, args...); err != nil {
		return fmt.Errorf("kontinue-runsc checkpoint %s failed: %w", containerID, err)
	}
	return nil
}

func (d *GVisorSandboxDriver) StopSandboxContainer(ctx context.Context, _ protocol.CarrierSlot, activeContainerID string) error {
	if activeContainerID == "" {
		return nil
	}
	root := "--root=" + d.cfg.RunscStateRoot
	if out, err := d.hostCommand(ctx, d.cfg.RunscWrapperBin, root, "delete", "--force", activeContainerID).CombinedOutput(); err != nil {
		return fmt.Errorf("delete sandbox %s: %w (%s)", activeContainerID, err, out)
	}
	if err := d.releaseHostQuota(ctx, activeContainerID); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(d.cfg.BundlesDir, activeContainerID))
}

// An empty identity means the exact reference is missing from the local image store.
func (d *GVisorSandboxDriver) imageIdentity(ctx context.Context, imageRef string) (string, error) {
	out, err := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "ls", "name=="+strconv.Quote(imageRef)).Output()
	if err != nil {
		return "", fmt.Errorf("resolve image %s: %w", imageRef, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == imageRef {
			if len(fields) < 3 || !strings.HasPrefix(fields[2], "sha256:") {
				return "", fmt.Errorf("missing digest for %s", imageRef)
			}
			return imageRef + "@" + fields[2], nil
		}
	}
	return "", nil
}

func (d *GVisorSandboxDriver) ensureBaseRootfs(ctx context.Context, imageRef string) (string, error) {
	if imageRef == "" {
		return "", fmt.Errorf("baseImage is empty")
	}
	value, err, _ := d.rootfsLoads.Do(imageRef, func() (any, error) { return d.stageBaseRootfs(ctx, imageRef) })
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func (d *GVisorSandboxDriver) stageBaseRootfs(ctx context.Context, imageRef string) (root string, err error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinued", "hydrator.prepare_base_rootfs")
	defer func() { telemetry.EndSpan(span, err) }()
	identity, err := d.imageIdentity(ctx, imageRef)
	if err != nil {
		return "", err
	}
	if identity == "" {
		if out, err := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "pull", imageRef).CombinedOutput(); err != nil {
			return "", fmt.Errorf("ctr pull %s: %w (%s)", imageRef, err, out)
		}
		identity, err = d.imageIdentity(ctx, imageRef)
		if err != nil {
			return "", err
		}
		if identity == "" {
			return "", fmt.Errorf("pulled image %s has no local digest", imageRef)
		}
	}
	sum := sha256.Sum256([]byte(identity))
	imgKey := hex.EncodeToString(sum[:10])
	targetRootfs := filepath.Join(d.cfg.BaseRootfsCache, imgKey, "rootfs")
	readyMarker := filepath.Join(d.cfg.BaseRootfsCache, imgKey, ".ready")
	if _, err := os.Stat(readyMarker); err == nil {
		return targetRootfs, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	mntDir := filepath.Join(d.cfg.BaseRootfsCache, imgKey, "mnt")
	if err := d.removeImageMount(ctx, mntDir); err != nil {
		return "", err
	}
	if err := os.RemoveAll(targetRootfs); err != nil {
		return "", err
	}
	if err := os.MkdirAll(mntDir, 0o755); err != nil {
		return "", err
	}
	if err := os.MkdirAll(targetRootfs, 0o755); err != nil {
		return "", err
	}
	if err := d.copyMountedRootfs(ctx, imageRef, mntDir, targetRootfs); err != nil {
		return "", err
	}
	_ = os.MkdirAll(filepath.Join(targetRootfs, "root"), 0o755)
	_ = os.MkdirAll(filepath.Join(targetRootfs, "etc"), 0o755)
	// A mutable tag can move while ctr mounts it. Publish only a verified cache entry.
	current, err := d.imageIdentity(ctx, imageRef)
	if err != nil {
		return "", err
	}
	if current != identity {
		return "", fmt.Errorf("image %s changed while staging rootfs", imageRef)
	}
	if err := os.WriteFile(readyMarker, []byte(imageRef+"\n"), 0o644); err != nil {
		return "", err
	}
	return targetRootfs, nil
}

func (d *GVisorSandboxDriver) copyMountedRootfs(ctx context.Context, imageRef, mntDir, targetRootfs string) (err error) {
	// A cancelled or failed mount command can still have created a mount.
	defer func() {
		clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, d.removeImageMount(clean, mntDir))
	}()
	mntOut, err := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "mount", "--rw", imageRef, mntDir).CombinedOutput()
	if err != nil && strings.Contains(string(mntOut), "already exists") {
		_ = d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "snapshots", "rm", mntDir).Run()
		mntOut, err = d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "mount", "--rw", imageRef, mntDir).CombinedOutput()
	}
	if err != nil {
		return fmt.Errorf("ctr images mount %s: %w (%s)", imageRef, err, string(mntOut))
	}

	cpOut, cpErr := d.hostCommand(ctx, "cp", "-a", mntDir+"/.", targetRootfs+"/").CombinedOutput()
	if cpErr != nil {
		return fmt.Errorf("stage rootfs %s: %w (%s)", imageRef, cpErr, string(cpOut))
	}
	return nil
}

func (d *GVisorSandboxDriver) removeImageMount(ctx context.Context, dir string) error {
	// A non-recursive removal is safe for an empty staging directory.
	if err := os.Remove(dir); err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if out, err := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "unmount", dir).CombinedOutput(); err != nil {
		return fmt.Errorf("unmount image rootfs at %s: %w (%s)", dir, err, out)
	}
	return os.RemoveAll(dir)
}

func defaultCaps() []string {
	return []string{
		"CAP_CHOWN",
		"CAP_DAC_OVERRIDE",
		"CAP_FSETID",
		"CAP_FOWNER",
		"CAP_MKNOD",
		"CAP_NET_RAW",
		"CAP_SETGID",
		"CAP_SETUID",
		"CAP_SETFCAP",
		"CAP_SETPCAP",
		"CAP_NET_BIND_SERVICE",
		"CAP_SYS_CHROOT",
		"CAP_KILL",
		"CAP_AUDIT_WRITE",
		// Needed so ktd can pidfd_getfd + shutdown external sockets during freeze.
		"CAP_SYS_PTRACE",
	}
}
