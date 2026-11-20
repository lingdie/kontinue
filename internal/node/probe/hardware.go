package probe

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"

	"github.com/klauspost/cpuid/v2"
	"github.com/kontinue-sh/kontinue/pkg/protocol"
)

// DetectNodeFingerprint inspects host CPU architecture, instruction-set features, runsc version,
// and ARM64 PAC status (`arm64.nopauth`) so incompatible cross-node memory restores are prevented.
func DetectNodeFingerprint(runscBinary string) protocol.CompatibilityFingerprint {
	features := cpuid.CPU.FeatureSet()
	slices.Sort(features)

	sum := sha256.Sum256([]byte(runtime.GOARCH + ":" + strings.Join(features, ",")))
	profileHash := hex.EncodeToString(sum[:8])

	runscVer := "runsc-unknown"
	if runscBinary != "" {
		if out, err := exec.Command(runscBinary, "--version").Output(); err == nil {
			firstLine := strings.TrimSpace(strings.Split(string(out), "\n")[0])
			if firstLine != "" {
				runscVer = firstLine
			}
		}
	}

	noPauth := true
	if runtime.GOARCH == "arm64" && runtime.GOOS == "linux" {
		noPauth = checkLinuxArm64NoPauth()
	}

	return protocol.CompatibilityFingerprint{
		Architecture:   runtime.GOARCH,
		CPUProfileHash: profileHash,
		CPUFeatures:    features,
		RunscVersion:   runscVer,
		Arm64NoPauth:   noPauth,
	}
}

func checkLinuxArm64NoPauth() bool {
	if cmdline, err := os.ReadFile("/proc/cmdline"); err == nil {
		if strings.Contains(string(cmdline), "arm64.nopauth") {
			return true
		}
	}
	if cpuinfo, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		txt := string(cpuinfo)
		if strings.Contains(txt, " paca") || strings.Contains(txt, " pacg") {
			return false
		}
	}
	return true
}
