package vitaledge

import (
	"reflect"
	"testing"

	v1 "github.com/spaceqraft/vitaledge-go/api/proto/vitaledge/v1"
)

func TestDecodeValue(t *testing.T) {
	t.Parallel()

	value := &v1.Value{
		Kind: &v1.Value_MapValue{
			MapValue: &v1.MapValue{
				Values: map[string]*v1.Value{
					"name": {
						Kind: &v1.Value_StringValue{StringValue: "neo"},
					},
					"age": {
						Kind: &v1.Value_IntValue{IntValue: 42},
					},
					"tags": {
						Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
							{Kind: &v1.Value_StringValue{StringValue: "admin"}},
							{Kind: &v1.Value_NullValue{NullValue: &v1.NullValue{}}},
						}}},
					},
				},
			},
		},
	}

	got := DecodeValue(value)
	want := map[string]any{
		"name": "neo",
		"age":  int64(42),
		"tags": []any{"admin", nil},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeValue() mismatch\nwant: %#v\n got: %#v", want, got)
	}
}

func TestEncodeValueRoundTripNested(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"name":   "neo",
		"age":    42,
		"active": true,
		"score":  4.25,
		"tags":   []any{"admin", nil, int64(9)},
		"meta": map[string]any{
			"team": "ops",
		},
	}

	encoded, err := EncodeValue(input)
	if err != nil {
		t.Fatalf("EncodeValue() unexpected error: %v", err)
	}

	decoded := DecodeValue(encoded)
	decodedMap, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded value type mismatch: %T", decoded)
	}

	if decodedMap["name"] != "neo" {
		t.Fatalf("decoded string mismatch: %#v", decodedMap["name"])
	}
	if decodedMap["age"] != int64(42) {
		t.Fatalf("decoded int mismatch: %#v", decodedMap["age"])
	}
	if decodedMap["active"] != true {
		t.Fatalf("decoded bool mismatch: %#v", decodedMap["active"])
	}
	if decodedMap["score"] != float64(4.25) {
		t.Fatalf("decoded double mismatch: %#v", decodedMap["score"])
	}
}

func TestEncodeValueUnsupportedType(t *testing.T) {
	t.Parallel()

	_, err := EncodeValue(struct{ X int }{X: 1})
	if err == nil {
		t.Fatal("EncodeValue() expected unsupported type error")
	}
}

func TestNewResult(t *testing.T) {
	t.Parallel()

	resp := &v1.QueryResponse{
		Columns: []string{"id", "active"},
		Rows: []*v1.Row{
			{
				Values: map[string]*v1.Value{
					"id": {
						Kind: &v1.Value_StringValue{StringValue: "n-1"},
					},
					"active": {
						Kind: &v1.Value_BoolValue{BoolValue: true},
					},
				},
			},
		},
		Stats:    &v1.QueryStats{RowsReturned: 1, DurationMs: 7},
		Warnings: []*v1.Diagnostic{{Code: "warn/demo", Message: "demo warning"}},
	}

	got := newResult(resp)

	if !reflect.DeepEqual(got.Columns, []string{"id", "active"}) {
		t.Fatalf("columns mismatch: %#v", got.Columns)
	}
	if got.Stats.RowsReturned != 1 || got.Stats.DurationMS != 7 {
		t.Fatalf("stats mismatch: %#v", got.Stats)
	}
	if len(got.Rows) != 1 || got.Rows[0]["id"] != "n-1" || got.Rows[0]["active"] != true {
		t.Fatalf("rows mismatch: %#v", got.Rows)
	}
	if len(got.Warnings) != 1 || got.Warnings[0].Code != "warn/demo" {
		t.Fatalf("warnings mismatch: %#v", got.Warnings)
	}
}

func TestCypherRequestPreservesQueryAndAddsParameters(t *testing.T) {
	t.Parallel()

	client := &Client{tenant: DefaultTenant}
	req, err := client.cypherRequest("RETURN $movieTitle AS title", map[string]any{"movieTitle": "Wall Street"})
	if err != nil {
		t.Fatalf("cypherRequest() unexpected error: %v", err)
	}

	if req.GetInput().GetCypher() != "RETURN $movieTitle AS title" {
		t.Fatalf("query was unexpectedly rewritten: %q", req.GetInput().GetCypher())
	}

	param := req.GetParameters()["movieTitle"]
	if param == nil {
		t.Fatal("missing encoded parameter movieTitle")
	}
	if got := param.GetStringValue(); got != "Wall Street" {
		t.Fatalf("parameter mismatch: %q", got)
	}
}

func TestCreatePropertyIndexRequestBuildsExpectedPayload(t *testing.T) {
	t.Parallel()

	client := &Client{tenant: "acme"}
	req, err := client.createPropertyIndexRequest("Movie", "movie_id", true)
	if err != nil {
		t.Fatalf("createPropertyIndexRequest() unexpected error: %v", err)
	}

	if req.GetTenant() != "acme" {
		t.Fatalf("tenant mismatch: %q", req.GetTenant())
	}
	if req.GetSchema() != "Movie" {
		t.Fatalf("schema mismatch: %q", req.GetSchema())
	}
	if req.GetProperty() != "movie_id" {
		t.Fatalf("property mismatch: %q", req.GetProperty())
	}
	if !req.GetIfNotExists() {
		t.Fatal("expected if_not_exists=true")
	}
}

func TestCreatePropertyIndexRequestValidation(t *testing.T) {
	t.Parallel()

	client := &Client{tenant: DefaultTenant}

	if _, err := client.createPropertyIndexRequest("", "movie_id", true); err == nil {
		t.Fatal("expected error for empty schema")
	}

	if _, err := client.createPropertyIndexRequest("Movie", "", true); err == nil {
		t.Fatal("expected error for empty property")
	}
}
