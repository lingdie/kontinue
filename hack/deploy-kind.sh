#!/usr/bin/env bash
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
CLUSTER_NAME="${CLUSTER_NAME:-kontinue-dev}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

HOST_ARCH="$(uname -m)"
if [[ "${HOST_ARCH}" == "arm64" || "${HOST_ARCH}" == "aarch64" ]]; then
  GVISOR_ARCH="aarch64"
  GO_ARCH="arm64"
else
  GVISOR_ARCH="x86_64"
  GO_ARCH="amd64"
fi

mkdir -p /tmp/kontinue-kind-shared-cas "${ROOT_DIR}/bin/linux-${GO_ARCH}"
chmod +x "${ROOT_DIR}/hack/setup-kind-cilium.sh"

needs_cluster=0
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  needs_cluster=1
elif [[ "${KIND_RECREATE:-0}" == "1" ]]; then
  needs_cluster=1
elif kubectl --context "kind-${CLUSTER_NAME}" -n kube-system get ds kindnet >/dev/null 2>&1; then
  echo "==> Existing cluster still uses kindnet; recreating for Cilium (set KIND_RECREATE=0 to skip)..."
  needs_cluster=1
elif ! kubectl --context "kind-${CLUSTER_NAME}" -n kube-system get ds cilium >/dev/null 2>&1; then
  echo "==> Cilium not installed on existing cluster; recreating..."
  needs_cluster=1
fi

echo "==> [1/7] Ensuring Kind cluster '${CLUSTER_NAME}' with Cilium CNI..."
if [[ "${needs_cluster}" == "1" ]]; then
  if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
    kind delete cluster --name "${CLUSTER_NAME}"
  fi
  # Nodes stay NotReady until Cilium installs; do not --wait for Ready here.
  kind create cluster --config "${ROOT_DIR}/hack/kind-config.yaml"
fi
kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null
"${ROOT_DIR}/hack/setup-kind-cilium.sh"

echo "==> [2/7] Building host & Linux (${GO_ARCH}) Go + Rust binaries..."
make -C "${ROOT_DIR}" build build-linux-go

docker build -f "${ROOT_DIR}/build/Dockerfile.rust-linux" -t kontinue/rust-artifacts:dev "${ROOT_DIR}"
CID="$(docker create kontinue/rust-artifacts:dev)"
docker cp "${CID}:/out/ktd" "${ROOT_DIR}/bin/linux-${GO_ARCH}/ktd"
docker cp "${CID}:/out/kontinue-runsc" "${ROOT_DIR}/bin/linux-${GO_ARCH}/kontinue-runsc"
docker cp "${CID}:/out/kontinue-arena-helper" "${ROOT_DIR}/bin/linux-${GO_ARCH}/kontinue-arena-helper"
docker rm "${CID}" >/dev/null
chmod +x "${ROOT_DIR}/bin/linux-${GO_ARCH}/"{ktd,kontinue-runsc,kontinue-arena-helper}

echo "==> [3/7] Building Kontinue service, infra, Carrier pause & Python Agent images..."
docker build --provenance=false -f "${ROOT_DIR}/build/Dockerfile.carrier" -t kontinue/carrier:dev "${ROOT_DIR}"
docker build --provenance=false -f "${ROOT_DIR}/build/Dockerfile.infra-redis" -t kontinue/redis:dev "${ROOT_DIR}"
docker build --provenance=false -f "${ROOT_DIR}/build/Dockerfile.infra-postgres" -t kontinue/postgres:dev "${ROOT_DIR}"

docker build --provenance=false -f "${ROOT_DIR}/build/Dockerfile.go-service" \
  --build-arg "BINARY_PATH=bin/linux-${GO_ARCH}/kontinue-apiserver" \
  -t kontinue/kontinue-apiserver:dev "${ROOT_DIR}"

docker build --provenance=false -f "${ROOT_DIR}/build/Dockerfile.go-service" \
  --build-arg "BINARY_PATH=bin/linux-${GO_ARCH}/kontinue-gateway" \
  -t kontinue/kontinue-gateway:dev "${ROOT_DIR}"

docker build --provenance=false -f "${ROOT_DIR}/build/Dockerfile.go-service" \
  --build-arg "BINARY_PATH=bin/linux-${GO_ARCH}/kontinued" \
  -t kontinue/kontinued:dev "${ROOT_DIR}"

cp "${ROOT_DIR}/bin/linux-${GO_ARCH}/ktd" "${ROOT_DIR}/examples/python-agent-template/ktd"
docker build --provenance=false -t kontinue/python-agent:dev "${ROOT_DIR}/examples/python-agent-template"
rm -f "${ROOT_DIR}/examples/python-agent-template/ktd"

echo "==> [4/7] Importing images into all Kind nodes..."
for NODE in $(kind get nodes --name "${CLUSTER_NAME}"); do
  docker save \
    kontinue/carrier:dev \
    kontinue/redis:dev \
    kontinue/postgres:dev \
    kontinue/kontinue-apiserver:dev \
    kontinue/kontinue-gateway:dev \
    kontinue/kontinued:dev \
    kontinue/python-agent:dev | docker exec -i "${NODE}" ctr -n k8s.io images import - >/dev/null
done

echo "==> [5/7] Provisioning Kind worker nodes (gVisor bundle + kontinue-cri-shim + Kubelet CRI switch)..."
GVISOR_CACHE="${ROOT_DIR}/.gocache/gvisor-${GVISOR_ARCH}"
mkdir -p "${GVISOR_CACHE}/bin"
if [[ ! -x "${GVISOR_CACHE}/bin/runsc" ]]; then
  echo "  -> Downloading gVisor (${GVISOR_ARCH}) bundle..."
  curl -fsSL "https://storage.googleapis.com/gvisor/releases/release/latest/${GVISOR_ARCH}/gvisor.tar.zstd" -o "${GVISOR_CACHE}/gvisor.tar.zstd"
  docker run --rm -v "${GVISOR_CACHE}:/gv" alpine:latest sh -c "apk add --no-cache zstd tar >/dev/null && tar --zstd -xf /gv/gvisor.tar.zstd -C /gv/bin"
fi

for NODE in $(kind get nodes --name "${CLUSTER_NAME}" | grep worker); do
  echo "  -> Configuring worker node: ${NODE}"
  docker cp "${GVISOR_CACHE}/bin/." "${NODE}:/usr/local/bin/"
  docker cp "${ROOT_DIR}/bin/linux-${GO_ARCH}/kontinue-runsc" "${NODE}:/usr/local/bin/kontinue-runsc"
  docker cp "${ROOT_DIR}/bin/linux-${GO_ARCH}/kontinue-arena-helper" "${NODE}:/usr/local/bin/kontinue-arena-helper"
  docker cp "${ROOT_DIR}/bin/linux-${GO_ARCH}/kontinue-cri-shim" "${NODE}:/usr/local/bin/kontinue-cri-shim"

  docker exec "${NODE}" bash -c '
    set -e
    chmod +x /usr/local/bin/runsc /usr/local/bin/containerd-shim-runsc-v1 /usr/local/bin/kontinue-runsc /usr/local/bin/kontinue-arena-helper /usr/local/bin/kontinue-cri-shim
    mkdir -p /run/kontinue /run/kontinue/runsc /run/kontinue-runsc /run/kontinue-arena/l1-tmpfs
    mkdir -p /var/lib/kontinue-arena/cas /var/lib/kontinue-arena/views /var/lib/kontinue-arena/bundles /var/lib/kontinue-arena/base-rootfs /var/lib/kubelet/checkpoints
    if ! mountpoint -q /run/kontinue-arena/l1-tmpfs; then
      mount -t tmpfs -o size=2G tmpfs /run/kontinue-arena/l1-tmpfs
    fi

    cat > /etc/systemd/system/kontinue-cri-shim.service <<EOF
[Unit]
Description=Kontinue Ultra-Thin CRI Shim Proxy
After=containerd.service
Requires=containerd.service

[Service]
ExecStart=/usr/local/bin/kontinue-cri-shim --listen-sock=/run/kontinue/cri.sock --upstream-sock=/run/containerd/containerd.sock --node-grpc=127.0.0.1:9090 --admin-sock=/run/kontinue/cri-shim-admin.sock
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl restart kontinue-cri-shim.service

    for i in $(seq 1 30); do
      [ -S /run/kontinue/cri.sock ] && break
      sleep 0.2
    done

    if [ -f /var/lib/kubelet/kubeadm-flags.env ]; then
      sed -i "s|unix:///run/containerd/containerd.sock|unix:///run/kontinue/cri.sock|g" /var/lib/kubelet/kubeadm-flags.env
    fi
    if [ -f /var/lib/kubelet/config.yaml ]; then
      sed -i "s|unix:///run/containerd/containerd.sock|unix:///run/kontinue/cri.sock|g" /var/lib/kubelet/config.yaml
    fi
    systemctl restart kubelet
  '
done

echo "==> [6/7] Waiting for nodes Ready (Cilium datapath)..."
kubectl wait --for=condition=Ready nodes --all --timeout=180s

echo "==> [7/7] Applying Kontinue Kubernetes manifests..."
kubectl apply -f "${ROOT_DIR}/deploy/00-namespace-rbac.yaml"
"${ROOT_DIR}/hack/ensure-kind-credentials.sh"
kubectl apply -f "${ROOT_DIR}/deploy/01-runtimeclass.yaml"
kubectl apply -f "${ROOT_DIR}/deploy/02-infra-dev.yaml"

kubectl -n kontinue-system rollout status deploy/kontinue-redis --timeout=90s
kubectl -n kontinue-system rollout status deploy/kontinue-postgres --timeout=90s

kubectl apply -f "${ROOT_DIR}/deploy/05-minio.yaml"
kubectl apply -f "${ROOT_DIR}/deploy/03-daemonset-kontinued.yaml"
kubectl apply -f "${ROOT_DIR}/deploy/04-apiserver-gateway.yaml"

kubectl -n kontinue-system rollout restart ds/kontinued deploy/kontinue-apiserver deploy/kontinue-gateway
kubectl -n kontinue-system rollout status ds/kontinued --timeout=90s
kubectl -n kontinue-system rollout status deploy/kontinue-apiserver --timeout=90s
kubectl -n kontinue-system rollout status deploy/kontinue-gateway --timeout=90s

echo "=================================================================="
echo "Kontinue is LIVE on Kind (${CLUSTER_NAME})!"
echo "  API Server : http://127.0.0.1:18081"
echo "  Gateway    : http://127.0.0.1:18080"
echo "=================================================================="
