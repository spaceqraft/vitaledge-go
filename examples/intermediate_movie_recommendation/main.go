package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	vitaledge "github.com/spaceqraft/vitaledge-go"
)

/*
WARNING: large dataset, uses server-side hints for batch sizes but takes a long
time to load the data since this is frankly a stress tester for data set size.

To explore, reduce the size of the dataset by using a subset of the kaggle data
files.

Builds a graph of users, movies, and genres from standard MovieLens-format CSVs,
then generates recommendations via graph-native collaborative filtering enhanced
with decade-aware user preference weighting.

Graph model:
  (:Movie {movie_id, title, year, avg_rating, num_ratings, base_score})
  (:Genre {genre})
  (:User  {user_id})
  (:Movie)-[:GENRED]->(:Genre)
  (:User)-[:RATED   {rating, ts}]->(:Movie)
  (:User)-[:RECOMMENDED {score, rank}]->(:Movie)   ← written during recommendation

Recommendation approach:
  1. Score each movie with a Bayesian-weighted popularity score to reduce bias
     towards low-count movies with a single 5-star rating.
  2. For each user, traverse the graph to find peer users who rated the same films
     similarly (collaborative filtering via shared RATED edges).
  3. Collect movies rated highly by peers that the target user has not yet seen.
  4. In Python, apply a decade-affinity boost: users often prefer films from a
     particular era — a reflection of nostalgia, production style, or life stage
     when they first discovered a genre — not simply "newer is better".
  5. Store top-N recommendations as RECOMMENDED relationships in the graph so
     they can be queried like any other graph relationship.

Dataset:
  https://www.kaggle.com/datasets/parasharmanas/movie-recommendation-system
  Expected files:
    movies.csv  — movieId, title (e.g. "Toy Story (1995)"), genres (pipe-separated)
    ratings.csv — userId, movieId, rating, timestamp
*/

type movieRecord struct {
	movieID int
	title   string
	year    int
	genres  []string
}

type ratingRecord struct {
	userID  int
	movieID int
	rating  float64
	ts      int64
}

var yearSuffixPattern = regexp.MustCompile(`\((\d{4})\)\s*$`)

func main() {
	moviesPath := flag.String("movies", "", "Path to movies.csv")
	ratingsPath := flag.String("ratings", "", "Path to ratings.csv")
	host := flag.String("host", "localhost", "VitalEdge host")
	port := flag.Int("port", 7443, "VitalEdge gRPC port")
	tenant := flag.String("tenant", "movierec", "VitalEdge tenant")
	batchSize := flag.Int("batch-size", 1000, "Node/relationship ingest batch size")
	edgeBatchSize := flag.Int("edge-batch-size", 5000, "GENRED relationship ingest batch size")
	ratingsLimit := flag.Int("ratings-limit", 10000, "Cap ratings rows loaded (0 = all)")
	userSample := flag.Int("user-sample", 50, "Number of most-active users to generate recommendations")
	limit := flag.Int("limit", 10, "Top-N results per output table")
	flag.Parse()

	if strings.TrimSpace(*moviesPath) == "" || strings.TrimSpace(*ratingsPath) == "" {
		log.Fatal("--movies and --ratings are required")
	}

	movies, err := loadMovies(*moviesPath)
	if err != nil {
		log.Fatalf("load movies: %v", err)
	}
	ratings, err := loadRatings(*ratingsPath, *ratingsLimit)
	if err != nil {
		log.Fatalf("load ratings: %v", err)
	}
	if len(movies) == 0 || len(ratings) == 0 {
		log.Fatal("no data found: check CSV paths and column names")
	}

	fmt.Printf("Loaded %d movies and %d ratings\n", len(movies), len(ratings))

	target := fmt.Sprintf("%s:%d", *host, *port)
	client, err := vitaledge.New(target, vitaledge.WithTenant(*tenant))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	caps, err := client.Capabilities(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Resetting graph ...")
	if err := resetGraph(ctx, client); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Ensuring ingest lookup indexes ...")
	ensureIngestIndexes(ctx, client, caps.GetIndexDdlSupported())

	fmt.Println("Ingesting graph ...")
	if err := ingestGraph(ctx, client, movies, ratings, *batchSize, *edgeBatchSize); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Scoring movies ...")
	if err := scoreMovies(ctx, client, ratings, *batchSize); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Generating recommendations for top %d users ...\n", *userSample)
	if err := recommendForUsers(ctx, client, *userSample, *limit); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nResults:")
	if err := printTopOverall(ctx, client, *limit); err != nil {
		log.Fatal(err)
	}
	if err := printTopPerGenre(ctx, client, *limit); err != nil {
		log.Fatal(err)
	}
	if err := printTopRecentYear(ctx, client, *limit); err != nil {
		log.Fatal(err)
	}
	if err := printUserRecommendations(ctx, client, *limit, 5); err != nil {
		log.Fatal(err)
	}
}

func loadMovies(path string) ([]movieRecord, error) {
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
	index := makeHeaderIndex(headers)

	movieIDIdx, okMovieID := index["movieid"]
	titleIdx, okTitle := index["title"]
	genresIdx, okGenres := index["genres"]
	if !okMovieID || !okTitle || !okGenres {
		return nil, errors.New("movies.csv requires movieId, title, genres")
	}

	records := make([]movieRecord, 0, 2048)
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			continue
		}

		movieID := mustInt(row[movieIDIdx])
		if movieID == 0 {
			continue
		}
		title, year := parseYear(row[titleIdx])
		records = append(records, movieRecord{
			movieID: movieID,
			title:   title,
			year:    year,
			genres:  splitGenres(row[genresIdx]),
		})
	}

	return records, nil
}

func loadRatings(path string, limit int) ([]ratingRecord, error) {
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
	index := makeHeaderIndex(headers)

	userIDIdx, okUserID := index["userid"]
	movieIDIdx, okMovieID := index["movieid"]
	ratingIdx, okRating := index["rating"]
	timestampIdx, okTS := index["timestamp"]
	if !okUserID || !okMovieID || !okRating || !okTS {
		return nil, errors.New("ratings.csv requires userId, movieId, rating, timestamp")
	}

	records := make([]ratingRecord, 0, 8192)
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			continue
		}
		rec := ratingRecord{
			userID:  mustInt(row[userIDIdx]),
			movieID: mustInt(row[movieIDIdx]),
			rating:  mustFloat(row[ratingIdx]),
			ts:      int64(mustInt(row[timestampIdx])),
		}
		if rec.userID == 0 || rec.movieID == 0 {
			continue
		}
		records = append(records, rec)
		if limit > 0 && len(records) >= limit {
			break
		}
	}

	return records, nil
}

func makeHeaderIndex(headers []string) map[string]int {
	out := make(map[string]int, len(headers))
	for i, h := range headers {
		out[strings.ToLower(strings.TrimSpace(h))] = i
	}
	return out
}

func parseYear(rawTitle string) (string, int) {
	title := strings.TrimSpace(rawTitle)
	match := yearSuffixPattern.FindStringSubmatch(title)
	if len(match) < 2 {
		return title, 0
	}
	year := mustInt(match[1])
	clean := strings.TrimSpace(strings.TrimSuffix(title, match[0]))
	return clean, year
}

func splitGenres(raw string) []string {
	parts := strings.Split(raw, "|")
	genres := make([]string, 0, len(parts))
	for _, p := range parts {
		g := strings.TrimSpace(p)
		if g == "" || strings.EqualFold(g, "(no genres listed)") {
			continue
		}
		genres = append(genres, g)
	}
	return genres
}

func mustInt(raw string) int {
	v, _ := strconv.Atoi(strings.TrimSpace(raw))
	return v
}

func mustFloat(raw string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	return v
}

func resetGraph(ctx context.Context, client *vitaledge.Client) error {
	_, err := client.Execute(ctx, "MATCH (n:Movie|Genre|User) DETACH DELETE n", nil)
	return err
}

func ensureIngestIndexes(ctx context.Context, client *vitaledge.Client, supported bool) {
	if !supported {
		fmt.Println("  Index DDL not supported by this server; continuing without index creation")
		return
	}

	specs := []struct {
		itype    string
		schema   string
		property string
	}{
		{itype: "Vertex", schema: "Movie", property: "movie_id"},
		{itype: "Vertex", schema: "User", property: "user_id"},
		{itype: "Vertex", schema: "Genre", property: "genre"},
		{itype: "Vertex", schema: "Movie", property: "year"},
		{itype: "Vertex", schema: "Movie", property: "num_ratings"},
		{itype: "Edge", schema: "RATED", property: "rating"},
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
}

func ingestGraph(ctx context.Context, client *vitaledge.Client, movies []movieRecord, ratings []ratingRecord, batchSize int, edgeBatchSize int) error {
	if batchSize <= 0 {
		batchSize = 500
	}
	if edgeBatchSize <= 0 {
		edgeBatchSize = 5000
	}

	uniqueGenres := make(map[string]struct{})
	for _, m := range movies {
		for _, g := range m.genres {
			uniqueGenres[g] = struct{}{}
		}
	}
	genreItems := make([]map[string]any, 0, len(uniqueGenres))
	for g := range uniqueGenres {
		genreItems = append(genreItems, map[string]any{"genre": g})
	}
	sort.Slice(genreItems, func(i, j int) bool {
		return asString(genreItems[i]["genre"]) < asString(genreItems[j]["genre"])
	})

	movieItems := make([]map[string]any, 0, len(movies))
	genrePairs := make([]map[string]any, 0, len(movies)*2)
	for _, m := range movies {
		movieItems = append(movieItems, map[string]any{
			"movie_id": m.movieID,
			"title":    m.title,
			"year":     m.year,
		})
		for _, g := range m.genres {
			genrePairs = append(genrePairs, map[string]any{
				"movie_id": m.movieID,
				"genre":    g,
			})
		}
	}

	uniqueUsers := make(map[int]struct{})
	ratingItems := make([]map[string]any, 0, len(ratings))
	for _, r := range ratings {
		uniqueUsers[r.userID] = struct{}{}
		ratingItems = append(ratingItems, map[string]any{
			"user_id":  r.userID,
			"movie_id": r.movieID,
			"rating":   r.rating,
			"ts":       r.ts,
		})
	}
	userItems := make([]map[string]any, 0, len(uniqueUsers))
	for userID := range uniqueUsers {
		userItems = append(userItems, map[string]any{"user_id": userID})
	}

	stages := []struct {
		name      string
		parameter string
		batch     int
		rows      []map[string]any
		query     string
	}{
		{name: "genre nodes", parameter: "genres", batch: batchSize, rows: genreItems, query: "UNWIND $genres AS g CREATE (:Genre {genre: g.genre})"},
		{name: "movie nodes", parameter: "movies", batch: batchSize, rows: movieItems, query: "UNWIND $movies AS m CREATE (:Movie {movie_id: m.movie_id, title: m.title, year: m.year})"},
		{name: "genre edges", parameter: "pairs", batch: edgeBatchSize, rows: genrePairs, query: "UNWIND $pairs AS p MATCH (m:Movie {movie_id: p.movie_id}) MATCH (g:Genre {genre: p.genre}) CREATE (m)-[:GENRED]->(g)"},
		{name: "user nodes", parameter: "users", batch: batchSize, rows: userItems, query: "UNWIND $users AS u CREATE (:User {user_id: u.user_id})"},
		{name: "rating edges", parameter: "ratings", batch: batchSize, rows: ratingItems, query: "UNWIND $ratings AS r MATCH (u:User {user_id: r.user_id}) MATCH (m:Movie {movie_id: r.movie_id}) CREATE (u)-[:RATED {rating: r.rating, ts: r.ts}]->(m)"},
	}

	for _, stage := range stages {
		fmt.Printf("  %s: %d rows\n", stage.name, len(stage.rows))
		for _, chunk := range batchMaps(stage.rows, stage.batch) {
			_, err := client.Execute(ctx, stage.query, map[string]any{stage.parameter: chunk}, vitaledge.WithStats())
			if err != nil {
				return fmt.Errorf("%s: %w", stage.name, err)
			}
		}
	}

	return nil
}

func batchMaps(items []map[string]any, size int) [][]map[string]any {
	if len(items) == 0 {
		return nil
	}
	if size <= 0 {
		size = len(items)
	}
	chunks := make([][]map[string]any, 0, (len(items)+size-1)/size)
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}

func scoreMovies(ctx context.Context, client *vitaledge.Client, ratings []ratingRecord, batchSize int) error {
	type aggregate struct {
		sum   float64
		count int
	}
	byMovie := make(map[int]aggregate)
	totalSum := 0.0
	totalCount := 0
	for _, r := range ratings {
		a := byMovie[r.movieID]
		a.sum += r.rating
		a.count++
		byMovie[r.movieID] = a
		totalSum += r.rating
		totalCount++
	}

	globalAvg := 3.0
	if totalCount > 0 {
		globalAvg = totalSum / float64(totalCount)
	}
	const confidence = 25.0

	updates := make([]map[string]any, 0, len(byMovie))
	for movieID, a := range byMovie {
		avg := a.sum / float64(a.count)
		base := (confidence*globalAvg + avg*float64(a.count)) / (confidence + float64(a.count))
		updates = append(updates, map[string]any{
			"movie_id":    movieID,
			"avg_rating":  avg,
			"num_ratings": a.count,
			"base_score":  base,
		})
	}

	query := "UNWIND $updates AS u MATCH (m:Movie {movie_id: u.movie_id}) SET m.avg_rating = u.avg_rating, m.num_ratings = u.num_ratings, m.base_score = u.base_score"
	for _, chunk := range batchMaps(updates, batchSize) {
		_, err := client.Execute(ctx, query, map[string]any{"updates": chunk})
		if err != nil {
			return err
		}
	}
	return nil
}

func recommendForUsers(ctx context.Context, client *vitaledge.Client, userSample int, limit int) error {
	if userSample <= 0 {
		return nil
	}
	users, err := client.Execute(ctx, "MATCH (u:User)-[:RATED]->() RETURN u.user_id AS user_id, count(*) AS rated_count ORDER BY rated_count DESC LIMIT $n", map[string]any{"n": userSample})
	if err != nil {
		return err
	}

	for i, row := range users.Rows {
		userID := asInt(row["user_id"])
		affinity, err := getUserDecadeAffinities(ctx, client, userID)
		if err != nil {
			return err
		}

		candidates, err := client.Execute(ctx, `
MATCH (target:User {user_id: $user_id})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User)
WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5
WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff
WHERE shared_count >= 3
WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity
ORDER BY similarity DESC
LIMIT 30
MATCH (peer)-[rp:RATED]->(candidate:Movie)
WHERE rp.rating >= 4.0 AND NOT (target)-[:RATED]->(candidate)
RETURN candidate.movie_id AS movie_id,
       candidate.title AS title,
       candidate.year AS year,
       coalesce(candidate.base_score, 0.0) AS base_score,
       avg(rp.rating) AS peer_avg,
       count(rp) AS peer_count,
       sum(similarity) AS total_sim
ORDER BY total_sim DESC
LIMIT $candidate_limit`, map[string]any{"user_id": userID, "candidate_limit": limit * 4})
		if err != nil {
			return err
		}

		type recommendation struct {
			movieID int
			score   float64
		}
		scored := make([]recommendation, 0, len(candidates.Rows))
		for _, candidate := range candidates.Rows {
			year := asInt(candidate["year"])
			decade := 0
			if year > 0 {
				decade = (year / 10) * 10
			}
			collab := asFloat(candidate["peer_avg"]) * math.Log(1.0+asFloat(candidate["peer_count"])) * asFloat(candidate["total_sim"])
			decadeBoost := affinity[decade] * 0.5
			baseBoost := asFloat(candidate["base_score"]) * 0.3
			scored = append(scored, recommendation{movieID: asInt(candidate["movie_id"]), score: collab + decadeBoost + baseBoost})
		}
		sort.Slice(scored, func(a, b int) bool { return scored[a].score > scored[b].score })
		if len(scored) > limit {
			scored = scored[:limit]
		}

		payload := make([]map[string]any, 0, len(scored))
		for rank, rec := range scored {
			payload = append(payload, map[string]any{
				"user_id":  userID,
				"movie_id": rec.movieID,
				"score":    rec.score,
				"rank":     rank + 1,
			})
		}
		if len(payload) > 0 {
			_, err := client.Execute(ctx, "UNWIND $recs AS r MATCH (u:User {user_id: r.user_id}) MATCH (m:Movie {movie_id: r.movie_id}) CREATE (u)-[rec:RECOMMENDED]->(m) SET rec.score = r.score, rec.rank = r.rank", map[string]any{"recs": payload})
			if err != nil {
				return err
			}
		}

		fmt.Printf("  [%d/%d] user_id=%d recommendations=%d\n", i+1, len(users.Rows), userID, len(payload))
	}

	return nil
}

func getUserDecadeAffinities(ctx context.Context, client *vitaledge.Client, userID int) (map[int]float64, error) {
	result, err := client.Execute(ctx, "MATCH (u:User {user_id: $user_id})-[r:RATED]->(m:Movie) WHERE m.year > 0 RETURN m.year AS year, avg(r.rating) AS avg_rating", map[string]any{"user_id": userID})
	if err != nil {
		return nil, err
	}

	sums := make(map[int]float64)
	counts := make(map[int]int)
	for _, row := range result.Rows {
		year := asInt(row["year"])
		if year <= 0 {
			continue
		}
		decade := (year / 10) * 10
		sums[decade] += asFloat(row["avg_rating"])
		counts[decade]++
	}
	affinity := make(map[int]float64, len(sums))
	for decade, sum := range sums {
		affinity[decade] = sum / float64(counts[decade])
	}
	return affinity, nil
}

func printTopOverall(ctx context.Context, client *vitaledge.Client, limit int) error {
	result, err := client.Execute(ctx, `
MATCH (m:Movie)
WHERE m.num_ratings >= 1
RETURN m.title AS title, m.year AS year, m.avg_rating AS avg_rating, m.num_ratings AS num_ratings, m.base_score AS score
ORDER BY score DESC
LIMIT $limit`, map[string]any{"limit": limit})
	if err != nil {
		return err
	}
	printRows(fmt.Sprintf("Top %d Overall Movies", limit), result.Rows)
	return nil
}

func printTopPerGenre(ctx context.Context, client *vitaledge.Client, limit int) error {
	genres, err := client.Execute(ctx, "MATCH (g:Genre) RETURN g.genre AS genre ORDER BY g.genre", nil)
	if err != nil {
		return err
	}
	for _, row := range genres.Rows {
		genre := asString(row["genre"])
		if genre == "" {
			continue
		}
		result, queryErr := client.Execute(ctx, `
MATCH (m:Movie)-[:GENRED]->(g:Genre {genre: $genre})
WHERE m.num_ratings >= 1
RETURN m.title AS title, m.year AS year, m.base_score AS score
ORDER BY score DESC
LIMIT $limit`, map[string]any{"genre": genre, "limit": limit})
		if queryErr != nil {
			return queryErr
		}
		printRows(fmt.Sprintf("Top %d %s Movies", limit, genre), result.Rows)
	}
	return nil
}

func printTopRecentYear(ctx context.Context, client *vitaledge.Client, limit int) error {
	yearRes, err := client.Execute(ctx, "MATCH (m:Movie) WHERE m.year > 0 AND m.num_ratings >= 1 RETURN max(m.year) AS max_year", nil)
	if err != nil {
		return err
	}
	if len(yearRes.Rows) == 0 {
		printRows("Top Recent Year Movies", nil)
		return nil
	}
	year := asInt(yearRes.Rows[0]["max_year"])
	if year == 0 {
		printRows("Top Recent Year Movies", nil)
		return nil
	}
	rows, err := client.Execute(ctx, `
MATCH (m:Movie)
WHERE m.year = $year AND m.num_ratings >= 1
RETURN m.title AS title, m.year AS year, m.avg_rating AS avg_rating, m.base_score AS score
ORDER BY score DESC
LIMIT $limit`, map[string]any{"year": year, "limit": limit})
	if err != nil {
		return err
	}
	printRows(fmt.Sprintf("Top %d Movies from %d", limit, year), rows.Rows)
	return nil
}

func printUserRecommendations(ctx context.Context, client *vitaledge.Client, limit int, displayCount int) error {
	users, err := client.Execute(ctx, "MATCH (u:User)-[rec:RECOMMENDED]->() RETURN u.user_id AS user_id, count(rec) AS rec_count ORDER BY rec_count DESC LIMIT $n", map[string]any{"n": displayCount})
	if err != nil {
		return err
	}
	for _, row := range users.Rows {
		userID := asInt(row["user_id"])
		recs, recErr := client.Execute(ctx, `
MATCH (u:User {user_id: $user_id})-[rec:RECOMMENDED]->(m:Movie)
RETURN m.title AS title, m.year AS year, rec.score AS score, rec.rank AS rank
ORDER BY rank
LIMIT $limit`, map[string]any{"user_id": userID, "limit": limit})
		if recErr != nil {
			return recErr
		}
		printRows(fmt.Sprintf("Top %d Recommendations for User %d", limit, userID), recs.Rows)
	}
	return nil
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
		out, _ := strconv.Atoi(x)
		return out
	default:
		return 0
	}
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		out, _ := strconv.ParseFloat(x, 64)
		return out
	default:
		return 0
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func printRows(title string, rows []vitaledge.Row) {
	fmt.Printf("\n=== %s ===\n", title)
	if len(rows) == 0 {
		fmt.Println("  (no results)")
		return
	}
	for _, row := range rows {
		fmt.Printf("  %v\n", row)
	}
}
