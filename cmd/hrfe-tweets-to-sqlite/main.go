package main

import (
	"database/sql"
	"fmt"
	"html"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"golang.org/x/exp/maps"
	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "data.db?_time_format=sqlite")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := initDB(db); err != nil {
		log.Fatal(err)
	}

	consumerKey, consumerSecret := os.Getenv("TWITTER_CONSUMER_KEY"), os.Getenv("TWITTER_CONSUMER_SECRET")
	appToken, appSecret := os.Getenv("TWITTER_APP_TOKEN"), os.Getenv("TWITTER_APP_SECRET")

	oaConfig := oauth1.NewConfig(consumerKey, consumerSecret)
	oaToken := oauth1.NewToken(appToken, appSecret)
	cl := oaConfig.Client(oauth1.NoContext, oaToken)
	twc := twitter.NewClient(cl)

	for {
		max, err := maxTweetID(db)
		if err != nil {
			log.Fatal(err)
		}

		tweets, err := tweetsSince(twc, max)
		if err != nil {
			log.Fatal(err)
		}
		if len(tweets) == 0 {
			break
		}

		if err := process(db, tweets); err != nil {
			log.Fatal(err)
		}
	}

	for {
		min, err := minTweetID(db)
		if err != nil {
			log.Fatal(err)
		}

		tweets, err := tweetsUntil(twc, min)
		if err != nil {
			log.Fatal(err)
		}
		if len(tweets) == 0 {
			break
		}

		if err := process(db, tweets); err != nil {
			log.Fatal(err)
		}
	}
}

func process(db *sql.DB, tweets []twitter.Tweet) error {
	for _, tw := range tweets {
		in, err := parse(tw.FullText)
		if err != nil {
			return fmt.Errorf("tweet id=%v: %w", tw.ID, err)
		}

		createdAt, err := tw.CreatedAtTime()
		if err != nil {
			return fmt.Errorf("tweet id=%v: %w", tw.ID, err)
		}

		if _, err := db.Exec(
			"insert into incidents values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict (tweet_id) do nothing",
			in.id, in.location, in.community, in.typ, strings.Join(in.apparatuses, " "), strings.Join(in.stations, " "), createdAt, tw.ID, tw.FullText, createdAt,
		); err != nil {
			return fmt.Errorf("tweet id=%v: %w", tw.ID, err)
		}

		fmt.Printf("in: %+v createdAt: %v\n", in, createdAt)
	}
	return nil
}

func tweetsSince(twc *twitter.Client, id int64) ([]twitter.Tweet, error) {
	params := &twitter.UserTimelineParams{
		ScreenName: "HRFE_Incidents",
		TweetMode:  "extended",
		SinceID:    id,
	}
	tweets, _, err := twc.Timelines.UserTimeline(params)
	if err != nil {
		return nil, err
	}
	return tweets, nil
}

func tweetsUntil(twc *twitter.Client, id int64) ([]twitter.Tweet, error) {
	params := &twitter.UserTimelineParams{
		ScreenName: "HRFE_Incidents",
		TweetMode:  "extended",
		MaxID:      id - 1,
	}
	tweets, _, err := twc.Timelines.UserTimeline(params)
	if err != nil {
		return nil, err
	}
	return tweets, nil
}

func maxTweetID(db *sql.DB) (int64, error) {
	var max sql.NullInt64
	if err := db.QueryRow("select max(tweet_id) from incidents").Scan(&max); err != nil {
		return 0, err
	}
	return max.Int64, nil
}

func minTweetID(db *sql.DB) (int64, error) {
	var min sql.NullInt64
	if err := db.QueryRow("select min(tweet_id) from incidents").Scan(&min); err != nil {
		return 0, err
	}
	return min.Int64, nil
}

var multiSpaceRe = regexp.MustCompile(`\s{3,}`)

type incident struct {
	id          string
	location    string
	community   string
	typ         string
	apparatuses []string
	stations    []string
}

func parse(s string) (incident, error) {
	s = html.UnescapeString(s)
	lines := strings.Split(s, "\n")
	if len(lines) != 4 {
		return incident{}, fmt.Errorf("bad tweet with %v lines", len(lines))
	}
	loc := lines[1]
	loc = multiSpaceRe.ReplaceAllString(loc, "  ")
	var comm string
	locParts := strings.Split(loc, "  ")
	if len(locParts) == 2 {
		loc = strings.TrimSpace(locParts[0])
		comm = strings.TrimSpace(locParts[1])
	}

	in := incident{
		id:        lines[0],
		location:  loc,
		community: comm,
		typ:       lines[2],
	}

	apparatuses := make(map[string]struct{})
	stations := make(map[string]struct{})
	for _, f := range strings.Fields(lines[3]) {
		if strings.HasPrefix(f, "STN") {
			stations[f] = struct{}{}
			continue
		}
		apparatuses[f] = struct{}{}
	}

	in.apparatuses = maps.Keys(apparatuses)
	sort.Strings(in.apparatuses)

	in.stations = maps.Keys(stations)
	sort.Strings(in.stations)

	return in, nil
}

func initDB(db *sql.DB) error {
	if _, err := db.Exec("create table if not exists incidents (id text, location text, community text, type text, apparatuses text, station text, created_at datetime, tweet_id integer UNIQUE, tweet_text text, tweet_created_at datetime)"); err != nil {
		return err
	}
	return nil
}
