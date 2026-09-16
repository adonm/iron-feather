#!/usr/bin/env bash
# One-time S3 bootstrap inside kind: Garage layout, lake bucket, access key.
# Writes the `lake-s3` Secret consumed by the ZeroFS gateways. In prod this
# step does not exist (real S3 + IRSA/external-secrets instead).
set -euo pipefail

NS="${NS:-lake}"
GARAGE="${GARAGE:-lake-garage}"
SECRET="${SECRET:-lake-s3}"

echo "waiting for garage..."
for _ in $(seq 1 60); do
  kubectl -n "$NS" exec "statefulset/$GARAGE" -c garage -- /garage status >/dev/null 2>&1 && break
  sleep 2
done
G="kubectl -n $NS exec statefulset/$GARAGE -c garage -- /garage"

if $G status 2>/dev/null | grep -q "NO ROLE ASSIGNED"; then
  ID=$($G status 2>/dev/null | grep -oE '^[0-9a-f]{16}' | head -1)
  $G layout assign -z dc1 -c 10G "$ID" >/dev/null
  VER=$($G layout show 2>/dev/null | grep -oE 'Current cluster layout version: [0-9]+' | grep -oE '[0-9]+')
  $G layout apply --version "$((VER + 1))" >/dev/null
  echo "layout applied"
fi
$G bucket list 2>/dev/null | grep -q '\blake\b' || $G bucket create lake >/dev/null
echo "bucket ready"
if kubectl -n "$NS" get secret "$SECRET" >/dev/null 2>&1; then
  echo "secret $SECRET exists, leaving it"
  exit 0
fi
OUT=$($G key create rig-key 2>/dev/null)
AK=$(echo "$OUT" | awk '/Key ID:/{print $3}')
SK=$(echo "$OUT" | awk '/Secret key:/{print $3}')
$G bucket allow lake --read --write --key rig-key >/dev/null
kubectl -n "$NS" create secret generic "$SECRET" \
  --from-literal=AWS_ACCESS_KEY_ID="$AK" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$SK" >/dev/null
echo "secret $SECRET written"
