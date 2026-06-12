#!/usr/bin/env bash
# Regenerate Go gRPC stubs from the VitalEdge proto definitions.
# Run from the vitaledge-go repository root.
set -euo pipefail

VITALEDGE_PROTO_ROOT="${VITALEDGE_PROTO_ROOT:-${HOME}/go/src/vitaledge/api/proto}"
PROTO_FILE_REL="${PROTO_FILE_REL:-vitaledge/v1/query.proto}"

SRC_PROTO_FILE="${VITALEDGE_PROTO_ROOT}/${PROTO_FILE_REL}"
VENDORED_PROTO_FILE="api/proto/vitaledge/v1/query.proto"
OUT_DIR="api/proto/vitaledge/v1"

if [[ ! -f "${SRC_PROTO_FILE}" ]]; then
  echo "error: proto file not found: ${SRC_PROTO_FILE}" >&2
  echo "hint: set VITALEDGE_PROTO_ROOT and/or PROTO_FILE_REL" >&2
  exit 1
fi

if ! command -v protoc >/dev/null 2>&1; then
  echo "error: protoc is required but was not found in PATH" >&2
  exit 1
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
  echo "error: protoc-gen-go is required but was not found in PATH" >&2
  echo "hint: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
  exit 1
fi

if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
  echo "error: protoc-gen-go-grpc is required but was not found in PATH" >&2
  echo "hint: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

# Keep a vendored local copy of the proto so this client repo can build standalone.
cp "${SRC_PROTO_FILE}" "${VENDORED_PROTO_FILE}"

protoc \
  -I . \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  "${VENDORED_PROTO_FILE}"

echo "Regenerated: ${VENDORED_PROTO_FILE}, ${OUT_DIR}/query.pb.go, ${OUT_DIR}/query_grpc.pb.go"