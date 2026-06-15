package vitaledge

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	v1 "github.com/spaceqraft/vitaledge-go/api/proto/vitaledge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultTarget = "127.0.0.1:7443"
	DefaultTenant = "default"
	SDKVersion    = "0.1.0"
)

type Option func(*config)

type QueryOption func(*v1.RequestOptions)

type config struct {
	tenant        string
	clientContext *v1.ClientContext
	dialOptions   []grpc.DialOption
}

type Client struct {
	conn          *grpc.ClientConn
	ddl           v1.DdlServiceClient
	dml           v1.DmlServiceClient
	tenant        string
	clientContext *v1.ClientContext
}

type PreparedQuery struct {
	ParserVersion  string
	IRVersion      string
	Fingerprint    string
	Payload        []byte
	FallbackCypher string
}

type Result struct {
	Columns  []string
	Rows     []Row
	Stats    Stats
	Warnings []Diagnostic
	Raw      *v1.QueryResponse
}

type ExplainResult struct {
	JSON     []byte
	Stats    Stats
	Warnings []Diagnostic
	Raw      *v1.ExplainResponse
}

type Row map[string]any

type Stats struct {
	RowsReturned int64
	DurationMS   int64
}

type Diagnostic struct {
	Code    string
	Message string
}

type CreatePropertyIndexResult struct {
	Created         bool
	IndexedEntities int64
	RawVertex       *v1.CreateVertexPropertyIndexResponse
	RawEdge         *v1.CreateEdgePropertyIndexResponse
}

func New(target string, opts ...Option) (*Client, error) {
	if strings.TrimSpace(target) == "" {
		return nil, errors.New("target is required")
	}

	cfg := config{
		tenant: DefaultTenant,
		clientContext: &v1.ClientContext{
			SdkLanguage:     "go",
			SdkVersion:      SDKVersion,
			ProtocolVersion: "v1",
		},
		dialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	conn, err := grpc.NewClient(target, cfg.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("dial vitaledge: %w", err)
	}

	return &Client{
		conn:          conn,
		ddl:           v1.NewDdlServiceClient(conn),
		dml:           v1.NewDmlServiceClient(conn),
		tenant:        cfg.tenant,
		clientContext: cfg.clientContext,
	}, nil
}

func WithTenant(tenant string) Option {
	return func(cfg *config) {
		trimmed := strings.TrimSpace(tenant)
		if trimmed != "" {
			cfg.tenant = trimmed
		}
	}
}

func WithClientContext(clientContext *v1.ClientContext) Option {
	return func(cfg *config) {
		if clientContext != nil {
			cfg.clientContext = clientContext
		}
	}
}

func WithDialOptions(options ...grpc.DialOption) Option {
	return func(cfg *config) {
		cfg.dialOptions = append(cfg.dialOptions, options...)
	}
}

func WithReadOnly() QueryOption {
	return func(options *v1.RequestOptions) {
		options.ReadOnly = true
	}
}

func WithStats() QueryOption {
	return func(options *v1.RequestOptions) {
		options.IncludeStats = true
	}
}

func WithWarnings() QueryOption {
	return func(options *v1.RequestOptions) {
		options.IncludeWarnings = true
	}
}

func WithFallbackToCypher() QueryOption {
	return func(options *v1.RequestOptions) {
		options.AllowFallbackToCypher = true
	}
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Execute(ctx context.Context, query string, parameters map[string]any, opts ...QueryOption) (*Result, error) {
	req, err := c.cypherRequest(query, parameters, opts...)
	if err != nil {
		return nil, err
	}

	resp, err := c.dml.Execute(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	return newResult(resp), nil
}

func (c *Client) ExecutePrepared(ctx context.Context, prepared PreparedQuery, opts ...QueryOption) (*Result, error) {
	req, err := c.preparedRequest(prepared, opts...)
	if err != nil {
		return nil, err
	}

	resp, err := c.dml.Execute(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("execute prepared query: %w", err)
	}
	return newResult(resp), nil
}

func (c *Client) Explain(ctx context.Context, query string, opts ...QueryOption) (*ExplainResult, error) {
	req, err := c.cypherRequest(query, nil, opts...)
	if err != nil {
		return nil, err
	}

	resp, err := c.dml.Explain(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("explain query: %w", err)
	}

	return &ExplainResult{
		JSON:     append([]byte(nil), resp.GetExplainJson()...),
		Stats:    statsFromProto(resp.GetStats()),
		Warnings: diagnosticsFromProto(resp.GetWarnings()),
		Raw:      resp,
	}, nil
}

func (c *Client) Capabilities(ctx context.Context) (*v1.CapabilitiesResponse, error) {
	resp, err := c.dml.GetCapabilities(ctx, &v1.CapabilitiesRequest{})
	if err != nil {
		return nil, fmt.Errorf("get capabilities: %w", err)
	}
	return resp, nil
}

func (c *Client) CreateVertexPropertyIndex(ctx context.Context, schema string, property string, ifNotExists bool) (*CreatePropertyIndexResult, error) {
	req, err := c.createVertexPropertyIndexRequest(schema, property, ifNotExists)
	if err != nil {
		return nil, err
	}

	resp, err := c.ddl.CreateVertexPropertyIndex(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create property index: %w", err)
	}

	return &CreatePropertyIndexResult{
		Created:         resp.GetCreated(),
		IndexedEntities: resp.GetIndexedEntities(),
		RawVertex:       resp,
		RawEdge:         nil,
	}, nil
}

func (c *Client) CreateEdgePropertyIndex(ctx context.Context, schema string, property string, ifNotExists bool) (*CreatePropertyIndexResult, error) {
	req, err := c.createEdgePropertyIndexRequest(schema, property, ifNotExists)
	if err != nil {
		return nil, err
	}

	resp, err := c.ddl.CreateEdgePropertyIndex(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create property index: %w", err)
	}

	return &CreatePropertyIndexResult{
		Created:         resp.GetCreated(),
		IndexedEntities: resp.GetIndexedEntities(),
		RawVertex:       nil,
		RawEdge:         resp,
	}, nil
}

func (c *Client) cypherRequest(query string, parameters map[string]any, opts ...QueryOption) (*v1.QueryRequest, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, errors.New("query is required")
	}

	encodedParameters, err := encodeParameters(parameters)
	if err != nil {
		return nil, err
	}

	return c.buildRequest(&v1.QueryInput{
		Kind: &v1.QueryInput_Cypher{Cypher: trimmed},
	}, encodedParameters, opts...), nil
}

func (c *Client) preparedRequest(prepared PreparedQuery, opts ...QueryOption) (*v1.QueryRequest, error) {
	if strings.TrimSpace(prepared.ParserVersion) == "" {
		return nil, errors.New("prepared query parser version is required")
	}
	if strings.TrimSpace(prepared.IRVersion) == "" {
		return nil, errors.New("prepared query IR version is required")
	}
	if len(prepared.Payload) == 0 {
		return nil, errors.New("prepared query payload is required")
	}

	protoPrepared := &v1.PreparedQuery{
		ParserVersion:  strings.TrimSpace(prepared.ParserVersion),
		IrVersion:      strings.TrimSpace(prepared.IRVersion),
		Fingerprint:    strings.TrimSpace(prepared.Fingerprint),
		Payload:        append([]byte(nil), prepared.Payload...),
		FallbackCypher: strings.TrimSpace(prepared.FallbackCypher),
	}

	return c.buildRequest(&v1.QueryInput{
		Kind: &v1.QueryInput_Prepared{Prepared: protoPrepared},
	}, nil, opts...), nil
}

func (c *Client) createVertexPropertyIndexRequest(schema string, property string, ifNotExists bool) (*v1.CreateVertexPropertyIndexRequest, error) {
	trimmedSchema := strings.TrimSpace(schema)
	if trimmedSchema == "" {
		return nil, errors.New("schema is required")
	}

	trimmedProperty := strings.TrimSpace(property)
	if trimmedProperty == "" {
		return nil, errors.New("property is required")
	}

	return &v1.CreateVertexPropertyIndexRequest{
		Tenant:      c.tenant,
		Schema:      trimmedSchema,
		Property:    trimmedProperty,
		IfNotExists: ifNotExists,
	}, nil
}

func (c *Client) createEdgePropertyIndexRequest(schema string, property string, ifNotExists bool) (*v1.CreateEdgePropertyIndexRequest, error) {
	trimmedSchema := strings.TrimSpace(schema)
	if trimmedSchema == "" {
		return nil, errors.New("schema is required")
	}

	trimmedProperty := strings.TrimSpace(property)
	if trimmedProperty == "" {
		return nil, errors.New("property is required")
	}

	return &v1.CreateEdgePropertyIndexRequest{
		Tenant:      c.tenant,
		Schema:      trimmedSchema,
		Property:    trimmedProperty,
		IfNotExists: ifNotExists,
	}, nil
}

func (c *Client) buildRequest(input *v1.QueryInput, parameters map[string]*v1.Value, opts ...QueryOption) *v1.QueryRequest {
	requestOptions := &v1.RequestOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(requestOptions)
		}
	}

	return &v1.QueryRequest{
		Tenant:     c.tenant,
		Input:      input,
		Options:    requestOptions,
		Client:     cloneClientContext(c.clientContext),
		Parameters: parameters,
	}

}

func cloneClientContext(clientContext *v1.ClientContext) *v1.ClientContext {
	if clientContext == nil {
		return nil
	}
	return &v1.ClientContext{
		SdkLanguage:     clientContext.GetSdkLanguage(),
		SdkVersion:      clientContext.GetSdkVersion(),
		ProtocolVersion: clientContext.GetProtocolVersion(),
	}
}

func newResult(resp *v1.QueryResponse) *Result {
	if resp == nil {
		return &Result{}
	}

	rows := make([]Row, 0, len(resp.GetRows()))
	for _, row := range resp.GetRows() {
		rows = append(rows, rowFromProto(row))
	}

	return &Result{
		Columns:  append([]string(nil), resp.GetColumns()...),
		Rows:     rows,
		Stats:    statsFromProto(resp.GetStats()),
		Warnings: diagnosticsFromProto(resp.GetWarnings()),
		Raw:      resp,
	}
}

func rowFromProto(row *v1.Row) Row {
	if row == nil {
		return nil
	}

	decoded := make(Row, len(row.GetValues()))
	for key, value := range row.GetValues() {
		decoded[key] = DecodeValue(value)
	}
	return decoded
}

func statsFromProto(stats *v1.QueryStats) Stats {
	if stats == nil {
		return Stats{}
	}
	return Stats{
		RowsReturned: stats.GetRowsReturned(),
		DurationMS:   stats.GetDurationMs(),
	}
}

func diagnosticsFromProto(items []*v1.Diagnostic) []Diagnostic {
	if len(items) == 0 {
		return nil
	}

	decoded := make([]Diagnostic, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		decoded = append(decoded, Diagnostic{
			Code:    item.GetCode(),
			Message: item.GetMessage(),
		})
	}
	return decoded
}

func DecodeValue(value *v1.Value) any {
	if value == nil {
		return nil
	}

	switch kind := value.GetKind().(type) {
	case *v1.Value_BoolValue:
		return kind.BoolValue
	case *v1.Value_IntValue:
		return kind.IntValue
	case *v1.Value_DoubleValue:
		return kind.DoubleValue
	case *v1.Value_StringValue:
		return kind.StringValue
	case *v1.Value_BytesValue:
		return append([]byte(nil), kind.BytesValue...)
	case *v1.Value_ListValue:
		values := kind.ListValue.GetValues()
		decoded := make([]any, 0, len(values))
		for _, item := range values {
			decoded = append(decoded, DecodeValue(item))
		}
		return decoded
	case *v1.Value_MapValue:
		decoded := make(map[string]any, len(kind.MapValue.GetValues()))
		for key, item := range kind.MapValue.GetValues() {
			decoded[key] = DecodeValue(item)
		}
		return decoded
	case *v1.Value_NullValue:
		return nil
	default:
		return nil
	}
}

func encodeParameters(parameters map[string]any) (map[string]*v1.Value, error) {
	if len(parameters) == 0 {
		return nil, nil
	}

	encoded := make(map[string]*v1.Value, len(parameters))
	for key, raw := range parameters {
		value, err := EncodeValue(raw)
		if err != nil {
			return nil, fmt.Errorf("encode parameter %q: %w", key, err)
		}
		encoded[key] = value
	}

	return encoded, nil
}

func EncodeValue(value any) (*v1.Value, error) {
	switch x := value.(type) {
	case nil:
		return &v1.Value{Kind: &v1.Value_NullValue{NullValue: &v1.NullValue{}}}, nil
	case bool:
		return &v1.Value{Kind: &v1.Value_BoolValue{BoolValue: x}}, nil
	case int:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case int8:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case int16:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case int32:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case int64:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: x}}, nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return nil, errors.New("uint overflows int64")
		}
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case uint8:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case uint16:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case uint32:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case uint64:
		if x > math.MaxInt64 {
			return nil, errors.New("uint64 overflows int64")
		}
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(x)}}, nil
	case float32:
		return &v1.Value{Kind: &v1.Value_DoubleValue{DoubleValue: float64(x)}}, nil
	case float64:
		return &v1.Value{Kind: &v1.Value_DoubleValue{DoubleValue: x}}, nil
	case string:
		return &v1.Value{Kind: &v1.Value_StringValue{StringValue: x}}, nil
	case []byte:
		return &v1.Value{Kind: &v1.Value_BytesValue{BytesValue: append([]byte(nil), x...)}}, nil
	case []any:
		items := make([]*v1.Value, 0, len(x))
		for i, item := range x {
			encoded, err := EncodeValue(item)
			if err != nil {
				return nil, fmt.Errorf("list item %d: %w", i, err)
			}
			items = append(items, encoded)
		}
		return &v1.Value{Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: items}}}, nil
	case map[string]any:
		items := make(map[string]*v1.Value, len(x))
		for k, item := range x {
			encoded, err := EncodeValue(item)
			if err != nil {
				return nil, fmt.Errorf("map field %q: %w", k, err)
			}
			items[k] = encoded
		}
		return &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: items}}}, nil
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return &v1.Value{Kind: &v1.Value_NullValue{NullValue: &v1.NullValue{}}}, nil
	}

	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return &v1.Value{Kind: &v1.Value_NullValue{NullValue: &v1.NullValue{}}}, nil
		}
		return EncodeValue(rv.Elem().Interface())
	}

	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		items := make([]*v1.Value, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			encoded, err := EncodeValue(rv.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("list item %d: %w", i, err)
			}
			items = append(items, encoded)
		}
		return &v1.Value{Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: items}}}, nil
	}

	if rv.Kind() == reflect.Map {
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("unsupported map key type %s", rv.Type().Key())
		}
		items := make(map[string]*v1.Value, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			encoded, err := EncodeValue(iter.Value().Interface())
			if err != nil {
				return nil, fmt.Errorf("map field %q: %w", key, err)
			}
			items[key] = encoded
		}
		return &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: items}}}, nil
	}

	return nil, fmt.Errorf("unsupported parameter type %T", value)
}
