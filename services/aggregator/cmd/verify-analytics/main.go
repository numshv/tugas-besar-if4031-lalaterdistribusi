package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/analytics"
)

type auditPage struct {
	Items      []hazard.Event `json:"items"`
	NextCursor *string        `json:"next_cursor"`
	Partial    bool           `json:"partial"`
}

func main() {
	base := flag.String("base", "http://localhost:8083", "base URL that serves /login, /analytics and /internal/audit")
	user := flag.String("user", "analytics-viewer", "dashboard username")
	password := flag.String("password", os.Getenv("CLIENT_ANALYTICS_SECRET"), "dashboard password (default $CLIENT_ANALYTICS_SECRET)")
	auditToken := flag.String("audit-token", firstToken(os.Getenv("AUDIT_TOKENS")), "audit bearer token (default first of $AUDIT_TOKENS)")
	bucket := flag.String("bucket", "day", "bucket to compare (hour, day, week, month)")
	flag.Parse()
	if err := run(strings.TrimRight(*base, "/"), *user, *password, *auditToken, *bucket); err != nil {
		fmt.Fprintln(os.Stderr, "verify-analytics:", err)
		os.Exit(1)
	}
}

func firstToken(raw string) string {
	for _, t := range strings.Split(raw, ",") {
		if t = strings.TrimSpace(t); t != "" {
			return t
		}
	}
	return ""
}

var client = &http.Client{Timeout: 30 * time.Second}

func getJSON(target, bearer string, out any) error {
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d %s", target, res.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, out)
}

func login(base, user, password string) (string, error) {
	payload, _ := json.Marshal(map[string]string{"username": user, "password": password})
	res, err := client.Post(base+"/login", "application/json", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if res.StatusCode != http.StatusOK || json.NewDecoder(res.Body).Decode(&out) != nil || out.AccessToken == "" {
		return "", fmt.Errorf("login as %s failed: HTTP %d", user, res.StatusCode)
	}
	return out.AccessToken, nil
}

func run(base, user, password, auditToken, bucket string) error {
	if password == "" || auditToken == "" {
		return fmt.Errorf("need --password (or $CLIENT_ANALYTICS_SECRET) and --audit-token (or $AUDIT_TOKENS)")
	}
	access, err := login(base, user, password)
	if err != nil {
		return err
	}
	var dash analytics.Result
	if err := getJSON(base+"/analytics/data?"+url.Values{"range": {"all"}, "bucket": {bucket}}.Encode(), access, &dash); err != nil {
		return err
	}

	var acc analytics.Accumulator
	events, pages, partial := 0, 0, false
	cursor := ""
	for {
		q := url.Values{"limit": {"1000"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page auditPage
		if err := getJSON(base+"/internal/audit/events?"+q.Encode(), auditToken, &page); err != nil {
			return err
		}
		pages++
		partial = partial || page.Partial
		for _, e := range page.Items {
			acc.Add(analytics.Point{Category: analytics.Category(e.HazardType, e.Attributes), Severity: string(e.Severity), AreaName: e.AreaName, At: e.OccurredAt.UTC()})
			events++
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	audit := acc.Result(analytics.Query{AllTime: true, Bucket: bucket})

	fmt.Printf("dashboard: total=%d method=%s partial=%v as_of=%s\n", dash.Total, dash.Method, dash.Partial, dash.AsOf.Format(time.RFC3339))
	fmt.Printf("audit:     total=%d from %d pages partial=%v\n", events, pages, partial)
	if dash.Partial || partial {
		fmt.Println("WARNING: a side reported partial data; numbers are lower bounds and may differ")
	}

	var diffs []string
	cmp := func(what string, a, b int64) {
		if a != b {
			diffs = append(diffs, fmt.Sprintf("%s: dashboard %d, audit %d", what, a, b))
		}
	}
	cmp("total", dash.Total, audit.Total)
	for _, c := range analytics.Categories {
		cmp("category "+c, dash.ByCategory[c], audit.ByCategory[c])
	}
	for _, s := range analytics.Severities {
		cmp("severity "+s, dash.BySeverity[s], audit.BySeverity[s])
	}
	areas := map[string][2]int64{}
	for _, a := range dash.ByArea {
		v := areas[a.AreaName]
		v[0] = a.Count
		areas[a.AreaName] = v
	}
	for _, a := range audit.ByArea {
		v := areas[a.AreaName]
		v[1] = a.Count
		areas[a.AreaName] = v
	}
	names := make([]string, 0, len(areas))
	for n := range areas {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		cmp(fmt.Sprintf("area %q", n), areas[n][0], areas[n][1])
	}
	for _, c := range analytics.Categories {
		got := bucketMap(dash.Buckets, dash.Series[c])
		want := bucketMap(audit.Buckets, audit.Series[c])
		for t, n := range want {
			cmp(fmt.Sprintf("series %s @ %s", c, t), got[t], n)
		}
		for t, n := range got {
			if _, ok := want[t]; !ok {
				cmp(fmt.Sprintf("series %s @ %s", c, t), n, 0)
			}
		}
	}

	if len(diffs) > 0 {
		for _, d := range diffs {
			fmt.Println("DIFF", d)
		}
		return fmt.Errorf("%d mismatches (if events are still arriving, wait a few seconds and run again)", len(diffs))
	}
	fmt.Printf("OK: dashboard matches Audit /events for %d categories, %d severities, %d areas and every %s bucket\n",
		len(analytics.Categories), len(analytics.Severities), len(names), bucket)
	return nil
}

func bucketMap(buckets []time.Time, counts []int64) map[string]int64 {
	out := map[string]int64{}
	for i, t := range buckets {
		if i < len(counts) && counts[i] != 0 {
			out[t.UTC().Format(time.RFC3339)] = counts[i]
		}
	}
	return out
}
