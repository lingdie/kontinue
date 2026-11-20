package hydrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
)

type GVisorDriverConfig struct {
	RunscWrapperBin string
	RunscStateRoot  string
	BundlesDir      string
	BaseRootfsCache string
	CtrBinary       string
	UseHostNsenter  bool
}

// GVisorSandboxDriver runs standalone single-root gVisor sandboxes via Rust `kontinue-runsc`
// attached directly to each pre-warmed Carrier Pod's CNI network namespace (`--network=sandbox`).
type GVisorSandboxDriver struct {
	cfg      GVisorDriverConfig
	rootfsMu sync.Mutex
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
	if os.Getenv("KONTINUE_HOST_NSENTER") == "true" {
		cfg.UseHostNsenter = true
	}
	_ = os.MkdirAll(cfg.RunscStateRoot, 0o700)
	_ = os.MkdirAll(cfg.BundlesDir, 0o700)
	_ = os.MkdirAll(cfg.BaseRootfsCache, 0o700)
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

	cid := fmt.Sprintf("kt-sbx-%s", opts.SandboxID)
	if opts.Slot.SlotID != "" {
		cid = fmt.Sprintf("kt-%s-%s", opts.SandboxID, filepath.Base(opts.Slot.SlotID))
	}
	bundleDir := filepath.Join(d.cfg.BundlesDir, cid)
	_ = os.RemoveAll(bundleDir)
	if err := os.MkdirAll(bundleDir, 0o700); err != nil {
		return "", "", err
	}

	annotations := telemetry.InjectAnnotations(ctx, map[string]string{
		protocol.AnnotationSandboxID: opts.SandboxID,
	})
	if opts.RestoreViewDir != "" && opts.RestoreMode == protocol.RestoreModeMemory {
		annotations[protocol.GVisorAnnotationRestoreHostPath] = opts.RestoreViewDir
		annotations[protocol.GVisorAnnotationRestoreDirect] = "false"
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
		"mounts": []map[string]any{
			{"destination": "/proc", "type": "proc", "source": "proc"},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "strictatime", "mode=755"}},
			{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
			{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "nodev", "mode=1777"}},
		},
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
	_ = d.hostCommand(ctx, d.cfg.RunscWrapperBin, "--root="+d.cfg.RunscStateRoot, "kill", activeContainerID, "KILL").Run()
	_ = d.hostCommand(ctx, d.cfg.RunscWrapperBin, "--root="+d.cfg.RunscStateRoot, "delete", "--force", activeContainerID).Run()
	_ = d.releaseHostQuota(ctx, activeContainerID)
	_ = os.RemoveAll(filepath.Join(d.cfg.BundlesDir, activeContainerID))
	return nil
}

func (d *GVisorSandboxDriver) imageIdentity(ctx context.Context, imageRef string) string {
	out, err := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "ls").Output()
	if err != nil {
		return imageRef
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == imageRef && strings.HasPrefix(fields[2], "sha256:") {
			return imageRef + "@" + fields[2]
		}
	}
	return imageRef
}

func (d *GVisorSandboxDriver) ensureBaseRootfs(ctx context.Context, imageRef string) (string, error) {
	if imageRef == "" {
		return "", fmt.Errorf("baseImage is empty")
	}
	d.rootfsMu.Lock()
	defer d.rootfsMu.Unlock()

	sum := sha256.Sum256([]byte(d.imageIdentity(ctx, imageRef)))
	imgKey := hex.EncodeToString(sum[:10])
	targetRootfs := filepath.Join(d.cfg.BaseRootfsCache, imgKey, "rootfs")
	readyMarker := filepath.Join(d.cfg.BaseRootfsCache, imgKey, ".ready")
	if _, err := os.Stat(readyMarker); err == nil {
		return targetRootfs, nil
	}

	mntDir := filepath.Join(d.cfg.BaseRootfsCache, imgKey, "mnt")
	_ = os.RemoveAll(filepath.Join(d.cfg.BaseRootfsCache, imgKey))
	if err := os.MkdirAll(mntDir, 0o755); err != nil {
		return "", err
	}
	if err := os.MkdirAll(targetRootfs, 0o755); err != nil {
		return "", err
	}

	checkOut, _ := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "ls", "-q").Output()
	if !containsLine(string(checkOut), imageRef) {
		if pullOut, pErr := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "pull", imageRef).CombinedOutput(); pErr != nil {
			return "", fmt.Errorf("ctr pull %s: %w (%s)", imageRef, pErr, string(pullOut))
		}
	}

	mntOut, err := d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "mount", "--rw", imageRef, mntDir).CombinedOutput()
	if err != nil && strings.Contains(string(mntOut), "already exists") {
		_ = d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "snapshots", "rm", mntDir).Run()
		mntOut, err = d.hostCommand(ctx, d.cfg.CtrBinary, "-n", "k8s.io", "images", "mount", "--rw", imageRef, mntDir).CombinedOutput()
	}
	if err != nil {
		return "", fmt.Errorf("ctr images mount %s: %w (%s)", imageRef, err, string(mntOut))
	}
	defer func() {
		_ = d.hostCommand(context.Background(), d.cfg.CtrBinary, "-n", "k8s.io", "images", "unmount", mntDir).Run()
		_ = os.RemoveAll(mntDir)
	}()

	cpOut, cpErr := d.hostCommand(ctx, "cp", "-a", mntDir+"/.", targetRootfs+"/").CombinedOutput()
	if cpErr != nil {
		return "", fmt.Errorf("stage rootfs %s: %w (%s)", imageRef, cpErr, string(cpOut))
	}
	_ = os.MkdirAll(filepath.Join(targetRootfs, "root"), 0o755)
	_ = os.MkdirAll(filepath.Join(targetRootfs, "etc"), 0o755)
	_ = os.WriteFile(readyMarker, []byte(imageRef+"\n"), 0o644)
	return targetRootfs, nil
}

func containsLine(lines, target string) bool {
	for _, l := range filepathSplitLines(lines) {
		if l == target {
			return true
		}
	}
	return false
}

func filepathSplitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			out = append(out, line)
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
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
