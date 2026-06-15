#!/usr/bin/env bash
# Regenerate Go gRPC stubs from the VitalEdge proto definitions.
# Run from the vitaledge-go repository root.
set -euo pipefail

VITALEDGE_PROTO_ROOT="${VITALEDGE_PROTO_ROOT:-${HOME}/go/src/vitaledge/api/proto}"
DDL_PROTO_FILE_REL="${PROTO_FILE_REL:-vitaledge/v1/ddl.proto}"
DML_PROTO_FILE_REL="${PROTO_FILE_REL:-vitaledge/v1/dml.proto}"

DDL_SRC_PROTO_FILE="${VITALEDGE_PROTO_ROOT}/${DDL_PROTO_FILE_REL}"
DDL_VENDORED_PROTO_FILE="api/proto/vitaledge/v1/ddl.proto"
DML_SRC_PROTO_FILE="${VITALEDGE_PROTO_ROOT}/${DML_PROTO_FILE_REL}"
DML_VENDORED_PROTO_FILE="api/proto/vitaledge/v1/dml.proto"
OUT_DIR="api/proto/vitaledge/v1"

if [[ ! -f "${DDL_SRC_PROTO_FILE}" ]]; then
  echo "error: proto file not found: ${DDL_SRC_PROTO_FILE}" >&2
  echo "hint: set VITALEDGE_PROTO_ROOT and/or DDL_PROTO_FILE_REL" >&2
  exit 1
fi
if [[ ! -f "${DML_SRC_PROTO_FILE}" ]]; then
  echo "error: proto file not found: ${DML_SRC_PROTO_FILE}" >&2
  echo "hint: set VITALEDGE_PROTO_ROOT and/or DML_PROTO_FILE_REL" >&2
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
cp "${DDL_SRC_PROTO_FILE}" "${DDL_VENDORED_PROTO_FILE}"
cp "${DML_SRC_PROTO_FILE}" "${DML_VENDORED_PROTO_FILE}"

protoc \
  -I . \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  "${DDL_VENDORED_PROTO_FILE}"

protoc \
  -I . \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  "${DML_VENDORED_PROTO_FILE}"

echo "Regenerated: ${DDL_VENDORED_PROTO_FILE}, ${OUT_DIR}/ddl.pb.go, ${OUT_DIR}/ddl_grpc.pb.go"
echo "Regenerated: ${DML_VENDORED_PROTO_FILE}, ${OUT_DIR}/dml.pb.go, ${OUT_DIR}/dml_grpc.pb.go"
