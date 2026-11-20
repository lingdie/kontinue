#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kontinue-dev}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> [1/4] Creating multi-node Kind cluster (${CLUSTER_NAME}) with /dev/fuse passthrough..."
if ! kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  kind create cluster --config "${ROOT_DIR}/hack/kind-config.yaml"
fi

HOST_ARCH="$(uname -m)"
if [[ "${HOST_ARCH}" == "arm64" || "${HOST_ARCH}" == "aarch64" ]]; then
  GVISOR_ARCH="aarch64"
  GO_ARCH="arm64"
else
  GVISOR_ARCH="x86_64"
  GO_ARCH="amd64"
fi

echo "==> [2/4] Cross-compiling Kontinue Linux (${GO_ARCH}) Go binaries..."
make -C "${ROOT_DIR}" build-linux-go

echo "==> [3/4] Installing gVisor (systrap) + kontinue-cri-shim on Kind worker nodes..."
for NODE in $(kind get nodes --name "${CLUSTER_NAME}" | grep worker); do
  echo "  -> Provisioning ${NODE} (${GVISOR_ARCH})..."
  docker cp "${ROOT_DIR}/bin/linux-${GO_ARCH}/kontinue-cri-shim" "${NODE}:/usr/local/bin/kontinue-cri-shim"

  docker exec "${NODE}" bash -c "
    set -e
    mkdir -p /run/kontinue /var/lib/kontinue-arena/cas /var/lib/kontinue-arena/views /run/kontinue-arena/l1-tmpfs
    if [ ! -x /usr/local/bin/runsc ]; then
      curl -fsSL https://storage.googleapis.com/gvisor/releases/release/latest/${GVISOR_ARCH}/runsc -o /usr/local/bin/runsc
      curl -fsSL https://storage.googleapis.com/gvisor/releases/release/latest/${GVISOR_ARCH}/containerd-shim-runsc-v1 -o /usr/local/bin/containerd-shim-runsc-v1
      chmod +x /usr/local/bin/runsc /usr/local/bin/containerd-shim-runsc-v1
    fi
    if ! grep -q 'plugins.\"io.containerd.grpc.v1.cri\".containerd.runtimes.kontinue-gvisor' /etc/containerd/config.toml; then
      cat >> /etc/containerd/config.toml <<'EOF'

[plugins.\"io.containerd.grpc.v1.cri\".containerd.runtimes.kontinue-gvisor]
  runtime_type = \"io.containerd.runsc.v1\"
[plugins.\"io.containerd.grpc.v1.cri\".containerd.runtimes.kontinue-gvisor.options]
  TypeUrl = \"io.containerd.runsc.v1.options\"
EOF
      systemctl restart containerd
    fi

    cat > /etc/systemd/system/kontinue-cri-shim.service <<'EOF'
[Unit]
Description=Kontinue Ultra-Thin CRI Shim Proxy
After=containerd.service
Requires=containerd.service

[Service]
ExecStart=/usr/local/bin/kontinue-cri-shim --listen-sock=/run/kontinue/cri.sock --upstream-sock=/run/containerd/containerd.sock --node-grpc=127.0.0.1:9090
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable --now kontinue-cri-shim.service
  "
done

echo "==> [4/4] Kind + gVisor + kontinue-cri-shim nodes ready! Apply deploy/*.yaml to launch services."
