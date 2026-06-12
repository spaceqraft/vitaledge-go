# vitaledge-go

`vitaledge-go` is a small Go client for VitalEdge's gRPC `QueryService`.

It targets the VitalEdge endpoint already exposed by the server at `127.0.0.1:7443` and defaults to plaintext gRPC, which matches the current server setup.

## Install

```bash
go get github.com/spaceqraft/vitaledge-go
```

## Quick Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	vitaledge "github.com/spaceqraft/vitaledge-go"
)

func main() {
	ctx := context.Background()

	client, err := vitaledge.New(
		vitaledge.DefaultTarget,
		vitaledge.WithTenant("acme"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = client.Close()
	}()

	result, err := client.Execute(
		ctx,
		`MATCH (n:Seed) RETURN n.id AS id LIMIT 5`,
		nil,
		vitaledge.WithReadOnly(),
		vitaledge.WithStats(),
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range result.Rows {
		fmt.Println(row["id"])
	}

	fmt.Printf("rows=%d durationMs=%d\n", result.Stats.RowsReturned, result.Stats.DurationMS)
}
```

## API Overview

- `New(target, opts...)` opens a gRPC client connection.
- `Execute(ctx, cypher, parameters, opts...)` runs a Cypher query with server-side parameter binding from a `map[string]any`.
- `ExecutePrepared(ctx, prepared, opts...)` sends a prepared-query payload.
- `Explain(ctx, cypher, opts...)` returns the raw explain JSON payload plus stats and warnings.
- `Capabilities(ctx)` fetches server protocol and prepared-query support metadata.

Parameter example:

```go
result, err := client.Execute(
	ctx,
	`MATCH (:Movie {title: $movieTitle})<-[r:ACTED_IN]-(p:Person)
	WHERE r.role CONTAINS $actorRole
	RETURN p.name AS actor, r.role AS role`,
	map[string]any{
		"movieTitle": "Wall Street",
		"actorRole":  "Fox",
	},
	vitaledge.WithReadOnly(),
)
```

Decoded row values map to Go values as follows:

- `bool_value` -> `bool`
- `int_value` -> `int64`
- `double_value` -> `float64`
- `string_value` -> `string`
- `bytes_value` -> `[]byte`
- `list_value` -> `[]any`
- `map_value` -> `map[string]any`
- `null_value` -> `nil`

## Examples

Run the converted Go examples from the repository root:

```bash
# Basic usage
go run ./examples/basic_usage

# Movie recommendation (requires MovieLens-style CSV files)
go run ./examples/intermediate_movie_recommendation \
	--movies /path/to/movies.csv \
	--ratings /path/to/ratings.csv

# Cyber threat detection (requires the Kaggle CSV file)
go run ./examples/advanced_cyber_threat_detection \
	--csv /path/to/cyberfeddefender_dataset.csv
```

## Notes

- The repository includes the VitalEdge protobuf definition and generated Go stubs under `api/proto/vitaledge/v1`, so it builds independently of the server repo.
- The default dial behavior is plaintext gRPC via `insecure.NewCredentials()` because the current VitalEdge gRPC server on port `7443` is not configured with TLS.

## Regenerate Protobuf Stubs

To resync the vendored proto file from the VitalEdge server repo and regenerate Go stubs:

```bash
./scripts/gen_proto.sh
```

Optional environment variables:

- `VITALEDGE_PROTO_ROOT` (default: `$HOME/go/src/vitaledge/api/proto`)
- `PROTO_FILE_REL` (default: `vitaledge/v1/query.proto`)

The script writes:

- `api/proto/vitaledge/v1/query.proto`
- `api/proto/vitaledge/v1/query.pb.go`
- `api/proto/vitaledge/v1/query_grpc.pb.go`