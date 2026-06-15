package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	vitaledge "github.com/spaceqraft/vitaledge-go"
)

/*
This example performs detection inside VitalEdge using graph + Cypher analytics.
The Kaggle fields Attack_Type and Label are explicitly held out from the threat
scoring logic and are only used for post-hoc evaluation.

Dataset:
https://www.kaggle.com/datasets/hussainsheikh03/cyber-threat-detection
*/

type flowRecord struct {
	flowID          int
	timestamp       string
	sourceIP        string
	destinationIP   string
	protocol        string
	packetLength    float64
	durationS       float64
	sourcePort      int
	destinationPort int
	bytesSent       float64
	bytesReceived   float64
	flags           string
	flowPacketsPerS float64
	flowBytesPerS   float64
	avgPacketSize   float64
	totalFwdPackets float64
	totalBwdPackets float64
	fwdHeaderLength float64
	bwdHeaderLength float64
	subFlowFwdBytes float64
	subFlowBwdBytes float64
	inbound         int
	attackType      string
	label           int
}

var requiredColumns = []string{
	"Timestamp",
	"Source_IP",
	"Destination_IP",
	"Protocol",
	"Packet_Length",
	"Duration",
	"Source_Port",
	"Destination_Port",
	"Bytes_Sent",
	"Bytes_Received",
	"Flags",
	"Flow_Packets/s",
	"Flow_Bytes/s",
	"Avg_Packet_Size",
	"Total_Fwd_Packets",
	"Total_Bwd_Packets",
	"Fwd_Header_Length",
	"Bwd_Header_Length",
	"Sub_Flow_Fwd_Bytes",
	"Sub_Flow_Bwd_Bytes",
	"Inbound",
	"Attack_Type",
	"Label",
}

func main() {
	csvPath := flag.String("csv", "", "Path to Kaggle CSV file")
	host := flag.String("host", "localhost", "VitalEdge host")
	port := flag.Int("port", 7443, "VitalEdge gRPC port")
	tenant := flag.String("tenant", "cyberthreat", "VitalEdge tenant")
	batchSize := flag.Int("batch-size", 250, "Flow ingest batch size")
	threshold := flag.Float64("threshold", 0.9, "Threat score threshold")
	limit := flag.Int("limit", 20, "Rows to print per result table")
	flag.Parse()

	if strings.TrimSpace(*csvPath) == "" {
		log.Fatal("--csv is required")
	}

	records, err := loadRecords(*csvPath)
	if err != nil {
		log.Fatal(err)
	}
	if len(records) == 0 {
		log.Fatal("no rows found in CSV")
	}

	target := fmt.Sprintf("%s:%d", *host, *port)
	client, err := vitaledge.New(target, vitaledge.WithTenant(*tenant))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	fmt.Printf("Loaded %d flow rows from %s\n", len(records), *csvPath)
	fmt.Println("Resetting graph and ingesting flow data ...")
	if err := resetGraph(ctx, client); err != nil {
		log.Fatal(err)
	}
	caps, err := client.Capabilities(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := ensureIngestIndexes(ctx, client, caps.GetIndexDdlSupported()); err != nil {
		log.Fatal(err)
	}
	if err := ingestFlows(ctx, client, records, *batchSize); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Scoring threats (without Attack_Type/Label features) ...")
	if err := scoreThreats(ctx, client, *threshold); err != nil {
		log.Fatal(err)
	}

	if err := runHuntingQueries(ctx, client, *limit); err != nil {
		log.Fatal(err)
	}
	if err := evaluateAgainstLabels(ctx, client, *limit); err != nil {
		log.Fatal(err)
	}
}

func loadRecords(path string) ([]flowRecord, error) {
	handle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = handle.Close() }()

	reader := csv.NewReader(handle)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}
	index := make(map[string]int, len(headers))
	for i, h := range headers {
		index[strings.TrimSpace(h)] = i
	}
	for _, col := range requiredColumns {
		if _, ok := index[col]; !ok {
			return nil, fmt.Errorf("CSV missing required column: %s", col)
		}
	}

	records := make([]flowRecord, 0, 8192)
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			continue
		}

		records = append(records, flowRecord{
			flowID:          len(records),
			timestamp:       cell(row, index, "Timestamp"),
			sourceIP:        fallback(cell(row, index, "Source_IP"), "unknown"),
			destinationIP:   fallback(cell(row, index, "Destination_IP"), "unknown"),
			protocol:        fallback(cell(row, index, "Protocol"), "UNKNOWN"),
			packetLength:    toFloat(cell(row, index, "Packet_Length")),
			durationS:       toFloat(cell(row, index, "Duration")),
			sourcePort:      toInt(cell(row, index, "Source_Port")),
			destinationPort: toInt(cell(row, index, "Destination_Port")),
			bytesSent:       toFloat(cell(row, index, "Bytes_Sent")),
			bytesReceived:   toFloat(cell(row, index, "Bytes_Received")),
			flags:           cell(row, index, "Flags"),
			flowPacketsPerS: toFloat(cell(row, index, "Flow_Packets/s")),
			flowBytesPerS:   toFloat(cell(row, index, "Flow_Bytes/s")),
			avgPacketSize:   toFloat(cell(row, index, "Avg_Packet_Size")),
			totalFwdPackets: toFloat(cell(row, index, "Total_Fwd_Packets")),
			totalBwdPackets: toFloat(cell(row, index, "Total_Bwd_Packets")),
			fwdHeaderLength: toFloat(cell(row, index, "Fwd_Header_Length")),
			bwdHeaderLength: toFloat(cell(row, index, "Bwd_Header_Length")),
			subFlowFwdBytes: toFloat(cell(row, index, "Sub_Flow_Fwd_Bytes")),
			subFlowBwdBytes: toFloat(cell(row, index, "Sub_Flow_Bwd_Bytes")),
			inbound:         toInt(cell(row, index, "Inbound")),
			attackType:      fallback(cell(row, index, "Attack_Type"), "Unknown"),
			label:           toInt(cell(row, index, "Label")),
		})
	}

	return records, nil
}

func cell(row []string, index map[string]int, key string) string {
	i := index[key]
	if i < 0 || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func fallback(value string, defaultValue string) string {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	return value
}

func toFloat(raw string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	return v
}

func toInt(raw string) int {
	v, _ := strconv.Atoi(strings.TrimSpace(raw))
	return v
}

func resetGraph(ctx context.Context, client *vitaledge.Client) error {
	_, err := client.Execute(ctx, "MATCH (f:Host|Flow) DETACH DELETE f", nil)
	return err
}

func ensureIngestIndexes(ctx context.Context, client *vitaledge.Client, supported bool) error {
	if !supported {
		fmt.Println("  Index DDL not supported by this server; continuing without index creation")
		return nil
	}

	specs := []struct {
		itype    string
		schema   string
		property string
	}{
		{itype: "Vertex", schema: "Host", property: "ip"},
		{itype: "Vertex", schema: "Flow", property: "protocol"},
		{itype: "Vertex", schema: "Flow", property: "detected_malicious"},
		{itype: "Vertex", schema: "Flow", property: "suspicious_flows"},
		{itype: "Vertex", schema: "Flow", property: "distinct_targets"},
		{itype: "Vertex", schema: "Flow", property: "distinct_ports"},
	}

	for _, spec := range specs {
		if spec.itype == "Vertex" {
			result, err := client.CreateVertexPropertyIndex(ctx, spec.schema, spec.property, true)
			if err != nil {
				fmt.Printf("  Index %s.%s: failed (%v)\n", spec.schema, spec.property, err)
				continue
			}
			state := "already exists"
			if result.Created {
				state = "created"
			}
			fmt.Printf("  Index %s.%s: %s (indexed_entities=%d)\n", spec.schema, spec.property, state, result.IndexedEntities)
		} else if spec.itype == "Edge" {
			result, err := client.CreateEdgePropertyIndex(ctx, spec.schema, spec.property, true)
			if err != nil {
				fmt.Printf("  Index %s.%s: failed (%v)\n", spec.schema, spec.property, err)
				continue
			}
			state := "already exists"
			if result.Created {
				state = "created"
			}
			fmt.Printf("  Index %s.%s: %s (indexed_entities=%d)\n", spec.schema, spec.property, state, result.IndexedEntities)
		}
	}

	return nil
}

func ingestFlows(ctx context.Context, client *vitaledge.Client, records []flowRecord, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 250
	}

	query := `
UNWIND $events AS e
MERGE (src:Host {ip: e.source_ip})
MERGE (dst:Host {ip: e.destination_ip})
CREATE (f:Flow {
    flow_id: e.flow_id,
    timestamp: e.timestamp,
    protocol: e.protocol,
    flags: e.flags,
    packet_length: e.packet_length,
    duration_s: e.duration_s,
    source_port: e.source_port,
    destination_port: e.destination_port,
    bytes_sent: e.bytes_sent,
    bytes_received: e.bytes_received,
    flow_packets_per_s: e.flow_packets_per_s,
    flow_bytes_per_s: e.flow_bytes_per_s,
    avg_packet_size: e.avg_packet_size,
    total_fwd_packets: e.total_fwd_packets,
    total_bwd_packets: e.total_bwd_packets,
    fwd_header_length: e.fwd_header_length,
    bwd_header_length: e.bwd_header_length,
    sub_flow_fwd_bytes: e.sub_flow_fwd_bytes,
    sub_flow_bwd_bytes: e.sub_flow_bwd_bytes,
    inbound: e.inbound,
    attack_type: e.attack_type,
    label: e.label
})
MERGE (src)-[:SENT]->(f)
MERGE (f)-[:TO]->(dst)
MERGE (src)-[:COMMUNICATES_WITH]->(dst)
`

	payloadRows := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		payloadRows = append(payloadRows, map[string]any{
			"flow_id":            rec.flowID,
			"timestamp":          rec.timestamp,
			"source_ip":          rec.sourceIP,
			"destination_ip":     rec.destinationIP,
			"protocol":           rec.protocol,
			"packet_length":      rec.packetLength,
			"duration_s":         rec.durationS,
			"source_port":        rec.sourcePort,
			"destination_port":   rec.destinationPort,
			"bytes_sent":         rec.bytesSent,
			"bytes_received":     rec.bytesReceived,
			"flags":              rec.flags,
			"flow_packets_per_s": rec.flowPacketsPerS,
			"flow_bytes_per_s":   rec.flowBytesPerS,
			"avg_packet_size":    rec.avgPacketSize,
			"total_fwd_packets":  rec.totalFwdPackets,
			"total_bwd_packets":  rec.totalBwdPackets,
			"fwd_header_length":  rec.fwdHeaderLength,
			"bwd_header_length":  rec.bwdHeaderLength,
			"sub_flow_fwd_bytes": rec.subFlowFwdBytes,
			"sub_flow_bwd_bytes": rec.subFlowBwdBytes,
			"inbound":            rec.inbound,
			"attack_type":        rec.attackType,
			"label":              rec.label,
		})
	}

	for _, chunk := range batchEvents(payloadRows, batchSize) {
		_, err := client.Execute(ctx, query, map[string]any{"events": chunk})
		if err != nil {
			return err
		}
	}
	return nil
}

func batchEvents(items []map[string]any, batchSize int) [][]map[string]any {
	if len(items) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(items)
	}
	chunks := make([][]map[string]any, 0, (len(items)+batchSize-1)/batchSize)
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}

func scoreThreats(ctx context.Context, client *vitaledge.Client, threshold float64) error {
	query := `
MATCH (f:Flow)
WITH
    f.protocol AS protocol,
    avg(f.bytes_sent) AS mean_bytes_sent,
    stDev(f.bytes_sent) AS stdev_bytes_sent,
    avg(f.bytes_received) AS mean_bytes_received,
    stDev(f.bytes_received) AS stdev_bytes_received,
    avg(f.flow_packets_per_s) AS mean_pps,
    stDev(f.flow_packets_per_s) AS stdev_pps,
    avg(f.flow_bytes_per_s) AS mean_bps,
    stDev(f.flow_bytes_per_s) AS stdev_bps,
    avg(f.packet_length) AS mean_packet_length,
    stDev(f.packet_length) AS stdev_packet_length
MATCH (f:Flow)
WHERE f.protocol = protocol
WITH
    f,
    abs((f.bytes_sent - mean_bytes_sent) / stdev_bytes_sent) AS z_bytes_sent,
    abs((f.bytes_received - mean_bytes_received) / stdev_bytes_received) AS z_bytes_received,
    abs((f.flow_packets_per_s - mean_pps) / stdev_pps) AS z_pps,
    abs((f.flow_bytes_per_s - mean_bps) / stdev_bps) AS z_bps,
    abs((f.packet_length - mean_packet_length) / stdev_packet_length) AS z_packet_length
WITH f, (z_bytes_sent + z_bytes_received + z_pps + z_bps + z_packet_length) / 5.0 AS threat_score
SET f.threat_score = threat_score,
    f.detected_malicious = CASE WHEN threat_score >= $threshold THEN true ELSE false END,
    f.model_version = "vitaledge-rulegraph-v3-cypher-anomaly"
RETURN count(f) AS updated_flows
`
	_, err := client.Execute(ctx, query, map[string]any{"threshold": threshold})
	return err
}

func runHuntingQueries(ctx context.Context, client *vitaledge.Client, limit int) error {
	if limit <= 0 {
		limit = 20
	}
	query := `
MATCH (src:Host)-[:SENT]->(f:Flow)
WHERE f.detected_malicious = true
RETURN "Top Suspicious Sources" AS report,
       src.ip AS source_ip,
       null AS destination_ip,
       count(f) AS suspicious_flows,
       null AS inbound_suspicious_flows,
       null AS distinct_targets,
       null AS distinct_ports,
       null AS distinct_sources,
       avg(f.threat_score) AS avg_score,
       max(f.threat_score) AS max_score
ORDER BY suspicious_flows DESC, avg_score DESC
LIMIT $limit_value
UNION ALL
MATCH (src:Host)-[:SENT]->(f:Flow)-[:TO]->(dst:Host)
WHERE f.detected_malicious = true
WITH src,
     count(f) AS suspicious_flows,
     count(DISTINCT dst.ip) AS distinct_targets,
     count(DISTINCT f.destination_port) AS distinct_ports,
     avg(f.threat_score) AS avg_score
WHERE suspicious_flows >= 8 AND distinct_targets >= 4 AND distinct_ports >= 3
RETURN "Possible Lateral Movement" AS report,
       src.ip AS source_ip,
       null AS destination_ip,
       suspicious_flows,
       null AS inbound_suspicious_flows,
       distinct_targets,
       distinct_ports,
       null AS distinct_sources,
       avg_score AS avg_score,
       null AS max_score
ORDER BY distinct_targets DESC, avg_score DESC
LIMIT $limit_value
UNION ALL
MATCH (src:Host)-[:SENT]->(f:Flow)-[:TO]->(dst:Host)
WHERE f.detected_malicious = true
RETURN "Destination Concentration" AS report,
       null AS source_ip,
       dst.ip AS destination_ip,
       null AS suspicious_flows,
       count(f) AS inbound_suspicious_flows,
       null AS distinct_targets,
       null AS distinct_ports,
       count(DISTINCT src.ip) AS distinct_sources,
       avg(f.threat_score) AS avg_score,
       null AS max_score
ORDER BY inbound_suspicious_flows DESC, distinct_sources DESC
LIMIT $limit_value
`

	result, err := client.Execute(ctx, query, map[string]any{"limit_value": limit}, vitaledge.WithStats())
	if err != nil {
		return err
	}

	grouped := map[string][]map[string]any{
		"Top Suspicious Sources":    {},
		"Possible Lateral Movement": {},
		"Destination Concentration": {},
	}
	for _, row := range result.Rows {
		report := asString(row["report"])
		switch report {
		case "Top Suspicious Sources":
			grouped[report] = append(grouped[report], map[string]any{
				"source_ip":        row["source_ip"],
				"suspicious_flows": row["suspicious_flows"],
				"avg_score":        row["avg_score"],
				"max_score":        row["max_score"],
			})
		case "Possible Lateral Movement":
			grouped[report] = append(grouped[report], map[string]any{
				"source_ip":        row["source_ip"],
				"suspicious_flows": row["suspicious_flows"],
				"distinct_targets": row["distinct_targets"],
				"distinct_ports":   row["distinct_ports"],
				"avg_score":        row["avg_score"],
			})
		case "Destination Concentration":
			grouped[report] = append(grouped[report], map[string]any{
				"destination_ip":           row["destination_ip"],
				"inbound_suspicious_flows": row["inbound_suspicious_flows"],
				"distinct_sources":         row["distinct_sources"],
				"avg_score":                row["avg_score"],
			})
		}
	}

	printMaps("Top Suspicious Sources", grouped["Top Suspicious Sources"])
	printMaps("Possible Lateral Movement", grouped["Possible Lateral Movement"])
	printMaps("Destination Concentration", grouped["Destination Concentration"])
	return nil
}

func evaluateAgainstLabels(ctx context.Context, client *vitaledge.Client, limit int) error {
	if limit <= 0 {
		limit = 20
	}

	confusionQuery := `
MATCH (f:Flow)
RETURN
  sum(CASE WHEN f.detected_malicious = true AND f.label = 1 THEN 1 ELSE 0 END) AS tp,
  sum(CASE WHEN f.detected_malicious = true AND f.label = 0 THEN 1 ELSE 0 END) AS fp,
  sum(CASE WHEN f.detected_malicious = false AND f.label = 1 THEN 1 ELSE 0 END) AS fn,
  sum(CASE WHEN f.detected_malicious = false AND f.label = 0 THEN 1 ELSE 0 END) AS tn
`
	confusionRows, err := client.Execute(ctx, confusionQuery, nil)
	if err != nil {
		return err
	}

	confusion := make([]map[string]any, 0, 1)
	if len(confusionRows.Rows) > 0 {
		row := confusionRows.Rows[0]
		tp := asInt(row["tp"])
		fp := asInt(row["fp"])
		fn := asInt(row["fn"])
		tn := asInt(row["tn"])
		precision := safeDivide(float64(tp), float64(tp+fp))
		recall := safeDivide(float64(tp), float64(tp+fn))
		f1 := safeDivide(float64(2*tp), float64(2*tp+fp+fn))
		confusion = append(confusion, map[string]any{
			"tp":        tp,
			"fp":        fp,
			"fn":        fn,
			"tn":        tn,
			"precision": round4(precision),
			"recall":    round4(recall),
			"f1":        round4(f1),
		})
	}

	attackSummaryQuery := `
MATCH (f:Flow)
RETURN f.attack_type AS attack_type,
    count(*) AS total,
    sum(CASE WHEN f.label = 1 THEN 1 ELSE 0 END) AS labeled_malicious,
    sum(CASE WHEN f.detected_malicious = true THEN 1 ELSE 0 END) AS detected_malicious,
    avg(f.threat_score) AS avg_score
ORDER BY avg_score DESC
`
	attackRows, err := client.Execute(ctx, attackSummaryQuery, nil)
	if err != nil {
		return err
	}

	breakdown := make([]map[string]any, 0, limit)
	for i, row := range attackRows.Rows {
		if i >= limit {
			break
		}
		total := asInt(row["total"])
		detected := asInt(row["detected_malicious"])
		breakdown = append(breakdown, map[string]any{
			"attack_type":        fallback(asString(row["attack_type"]), "Unknown"),
			"total":              total,
			"labeled_malicious":  asInt(row["labeled_malicious"]),
			"detected_malicious": detected,
			"avg_score":          row["avg_score"],
			"detected_rate":      round4(safeDivide(float64(detected), float64(total))),
		})
	}

	printMaps("Evaluation vs Held-Out Labels", confusion)
	printMaps("Attack-Type Comparison (Post-Hoc Only)", breakdown)
	return nil
}

func safeDivide(a float64, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func round4(v float64) float64 {
	return mathRound(v*10000) / 10000
}

func mathRound(v float64) float64 {
	if v < 0 {
		return float64(int64(v - 0.5))
	}
	return float64(int64(v + 0.5))
}

func asInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(x))
		return i
	default:
		return 0
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func printMaps(title string, rows []map[string]any) {
	fmt.Printf("\n=== %s ===\n", title)
	if len(rows) == 0 {
		fmt.Println("No rows returned")
		return
	}
	for _, row := range rows {
		fmt.Printf("%v\n", row)
	}
}
