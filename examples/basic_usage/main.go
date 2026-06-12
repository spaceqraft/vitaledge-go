package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	vitaledge "github.com/spaceqraft/vitaledge-go"
)

func main() {
	ctx := context.Background()

	client, err := vitaledge.New(
		vitaledge.DefaultTarget,
		vitaledge.WithTenant("basic_example"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = client.Close()
	}()

	caps, err := client.Capabilities(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Protocol version : %s\n", caps.GetProtocolVersion())
	fmt.Printf("Parser versions  : %v\n", caps.GetParserVersions())
	fmt.Printf("Prepared queries : %v\n\n", caps.GetPreparedQuerySupported())

	_, err = client.Execute(ctx, `
CREATE (a:Person {name: 'Alice', age: 30})
CREATE (b:Person {name: 'Bob', age: 52})
CREATE (c:Person {name: 'Charlie', age: 42})
CREATE (a)-[:KNOWS]->(b)
`, nil)
	if err != nil {
		log.Fatal(err)
	}

	result, err := client.Execute(ctx, "MATCH (p:Person) RETURN p LIMIT 5", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Columns : %v\n", result.Columns)
	for _, row := range result.Rows {
		fmt.Println(row)
	}

	plan, err := client.Explain(ctx, `
MATCH (p1:Person)-[r:KNOWS]->(p2:Person)
RETURN p1, r, p2
LIMIT 10
`)
	if err != nil {
		log.Fatal(err)
	}

	var pretty any
	if err := json.Unmarshal(plan.JSON, &pretty); err == nil {
		encoded, _ := json.MarshalIndent(pretty, "", "  ")
		fmt.Println(string(encoded))
	} else {
		fmt.Println(string(plan.JSON))
	}

	query := `
MATCH (p:Person {name: $personName})
RETURN p.name AS personName, $thisYear - p.age AS yearOfBirth
`
	params := map[string]any{
		"personName": "Bob",
		"thisYear":   time.Now().Year(),
	}

	bornResult, err := client.Execute(ctx, query, params)
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range bornResult.Rows {
		fmt.Printf("%v was born in %v\n", row["personName"], row["yearOfBirth"])
	}

	_, err = client.Execute(ctx, "MATCH (p:Person) DETACH DELETE p", nil)
	if err != nil {
		log.Fatal(err)
	}
}
