#!/usr/bin/env bash
# Install Cilium CNI into the kontinue-dev Kind cluster (kindnet disabled in kind-config.yaml).
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
CLUSTER_NAME="${CLUSTER_NAME:-kontinue-dev}"
CILIUM_VERSION="${CILIUM_VERSION:-1.16.5}"
CONTEXT="kind-${CLUSTER_NAME}"

kubectl config use-context "${CONTEXT}" >/dev/null

# Without kube-proxy / before CNI is up, pods cannot reach ClusterIP 10.96.0.1.
# Point Cilium at the Kind control-plane node IP + :6443 instead.
CONTROL_PLANE_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${CLUSTER_NAME}-control-plane" 2>/dev/null || true)"
if [[ -z "${CONTROL_PLANE_IP}" ]]; then
  CONTROL_PLANE_IP="$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
fi
if [[ -z "${CONTROL_PLANE_IP}" ]]; then
  echo "Unable to resolve Kind control-plane IP for k8sServiceHost" >&2
  exit 1
fi
echo "==> Using k8sServiceHost=${CONTROL_PLANE_IP}:6443"

HELM_SETS=(
  --set "ipam.mode=kubernetes"
  --set "kubeProxyReplacement=true"
  --set "k8sServiceHost=${CONTROL_PLANE_IP}"
  --set "k8sServicePort=6443"
  --set "operator.replicas=1"
  --set "hubble.enabled=false"
  --set "hubble.relay.enabled=false"
  --set "hubble.ui.enabled=false"
  --set "envoy.enabled=false"
  --set "routingMode=tunnel"
  --set "tunnelProtocol=vxlan"
  --set "cni.exclusive=true"
)

if kubectl -n kube-system get ds cilium >/dev/null 2>&1; then
  echo "==> Cilium DaemonSet present; upgrading values (k8sServiceHost) if needed..."
  if command -v helm >/dev/null 2>&1; then
    helm upgrade cilium cilium/cilium \
      --version "${CILIUM_VERSION}" \
      --namespace kube-system \
      "${HELM_SETS[@]}" \
      --reuse-values \
      --timeout 10m || true
  fi
else
  if command -v cilium >/dev/null 2>&1; then
    echo "==> Installing Cilium ${CILIUM_VERSION} via cilium-cli..."
    cilium install \
      --version "${CILIUM_VERSION}" \
      --set "ipam.mode=kubernetes" \
      --set "kubeProxyReplacement=true" \
      --set "k8sServiceHost=${CONTROL_PLANE_IP}" \
      --set "k8sServicePort=6443" \
      --set "operator.replicas=1" \
      --set "hubble.enabled=false" \
      --set "envoy.enabled=false" \
      --set "routingMode=tunnel" \
      --set "tunnelProtocol=vxlan" \
      --wait
  elif command -v helm >/dev/null 2>&1; then
    echo "==> Installing Cilium ${CILIUM_VERSION} via Helm..."
    helm repo add cilium https://helm.cilium.io/ >/dev/null 2>&1 || true
    helm repo update cilium >/dev/null
    helm upgrade --install cilium cilium/cilium \
      --version "${CILIUM_VERSION}" \
      --namespace kube-system \
      "${HELM_SETS[@]}" \
      --timeout 10m
  else
    echo "Need helm or cilium CLI. Install one of:" >&2
    echo "  brew install helm" >&2
    echo "  brew install cilium-cli" >&2
    exit 1
  fi
fi

echo "==> Waiting for Cilium agent pods..."
kubectl -n kube-system rollout status ds/cilium --timeout=300s
kubectl -n kube-system rollout status deploy/cilium-operator --timeout=180s

kubectl -n kube-system rollout restart deploy/coredns >/dev/null 2>&1 || true
kubectl -n kube-system rollout status deploy/coredns --timeout=180s || true

echo "==> Cilium ready on ${CONTEXT}"
kubectl get nodes -o wide
kubectl -n kube-system get pods -l k8s-app=cilium -o wide
