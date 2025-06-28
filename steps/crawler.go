package steps

import (
	"context"
	"net/http"
	"strings"
	"time"
	"encoding/json"
	"os"
	"path/filepath"
	"github.com/rasha-hantash/gdoc-pipeline/steps/types"
	"regexp"
	"io"
	"bytes"
	"fmt"
	"net/url"
	"crypto/sha1"
	"golang.org/x/net/html"
	"log"
	)

const (
    httpTimeout = 10 * time.Second
    maxDepth    = 3
)

var verbose = true


// -------------------- crawler helpers (regex, HTML, etc.) ------------------

var (
    docRe       = regexp.MustCompile(`docs\.google\.com/document/d/([^/?#]+)`) // id capture
    sheetRe     = regexp.MustCompile(`docs\.google\.com/spreadsheets/d/([^/?#]+)`) // id capture
    redirectRe  = regexp.MustCompile(`^https?://(www\.)?google\.com/url`)
    nonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)
    multiHyphen = regexp.MustCompile(`-{2,}`)
    titleTrimRE = regexp.MustCompile(`\s*-\s*Google (Docs?|Sheets?)\s*$`)
)

func RunCrawler(startURL string, outDir string, out chan<- string) {
    defer close(out)

    ctx := context.Background()
    httpCli := &http.Client{Timeout: httpTimeout}
    start := time.Now()

    queue := []types.Task{{Link: startURL, Depth: 0, Parent: outDir}}
    seen := map[string]string{}
    var totalDocs, totalSheets int

    for len(queue) > 0 {
        t := queue[0]
        queue = queue[1:]

        if t.Depth > maxDepth {
            continue
        }
        canonical := canonicalKey(t.Link)
        if canonical == "" {
            continue
        }
        if dir, dup := seen[canonical]; dup {
            writeRedirectMetadata(t.Parent, canonical, t.Link, dir, t.Depth)
            logf("↩︎ duplicate %s → redirect metadata", canonical)
            continue
        }

        switch {
        case strings.HasPrefix(canonical, "doc:"):
            nested, dir, err := scrapeDoc(ctx, httpCli, t)
            if err != nil {
                logf("WARN doc: %v", err)
                continue
            }
            seen[canonical] = dir
            totalDocs++
            out <- dir
            queue = append(queue, nested...)
        case strings.HasPrefix(canonical, "sheet:"):
            dir, err := scrapeSheet(ctx, httpCli, t)
            if err != nil {
                logf("WARN sheet: %v", err)
                continue
            }
            seen[canonical] = dir
            totalSheets++
            out <- dir
        }
    }
    logf("crawler finished in %s — %d docs, %d sheets", time.Since(start).Round(time.Millisecond), totalDocs, totalSheets)
}


func scrapeDoc(ctx context.Context, c *http.Client, t types.Task) ([]types.Task, string, error) {
    id := docRe.FindStringSubmatch(t.Link)[1]
    exportURL := fmt.Sprintf("https://docs.google.com/document/d/%s/export?format=html", id)

    resp, err := httpGet(ctx, c, exportURL)
    if err != nil {
        return nil, "", err
    }
    defer resp.Body.Close()

    data, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, "", err
    }
    root, err := html.Parse(bytes.NewReader(data))
    if err != nil {
        return nil, "", err
    }

    title := firstHrefText(root)
    if title == "" {
        title = extractHTMLTitle(root)
    }

    dir := filepath.Join(t.Parent, makeSlug(title, id))
    must(os.MkdirAll(dir, 0o755))

    must(os.WriteFile(filepath.Join(dir, "content.html"), data, 0o644))
    writeMetadata(dir, types.Metadata{Title: title, ID: id, SourceURL: t.Link, Depth: t.Depth, Type: "doc"})

    // Discover nested links
    var nested []types.Task
    var walk func(*html.Node)
    walk = func(n *html.Node) {
        if n.Type == html.ElementNode && n.Data == "a" {
            for _, a := range n.Attr {
                if a.Key == "href" {
                    href := canonicalLink(resolve(t.Link, a.Val))
                    if href != "" && (docRe.MatchString(href) || sheetRe.MatchString(href)) {
                        nested = append(nested, types.Task{Link: href, Depth: t.Depth + 1, Parent: dir})
                    }
                    break
                }
            }
        }
        for c := n.FirstChild; c != nil; c = c.NextSibling {
            walk(c)
        }
    }
    walk(root)
    logf("    saved Doc → %s (%d nested links)", dir, len(nested))
    return nested, dir, nil
}

func scrapeSheet(ctx context.Context, c *http.Client, t types.Task) (string, error) {
    id := sheetRe.FindStringSubmatch(t.Link)[1]
    title, _ := fetchSheetTitle(ctx, c, id)

    dir := filepath.Join(t.Parent, makeSlug(title, id))
    must(os.MkdirAll(dir, 0o755))

    csvURL := fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/export?format=csv", id)
    resp, err := httpGet(ctx, c, csvURL)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    outFile := filepath.Join(dir, "content.csv")
    f, err := os.Create(outFile)
    if err != nil {
        return "", err
    }
    _, err = io.Copy(f, resp.Body)
    f.Close()
    if err != nil {
        return "", err
    }

    writeMetadata(dir, types.Metadata{Title: title, ID: id, SourceURL: t.Link, Depth: t.Depth, Type: "sheet"})
    logf("    saved Sheet → %s", dir)
    return dir, nil
}


func logf(format string, v ...any) {
    if verbose {
        log.Printf(format, v...)
    }
}



func writeRedirectMetadata(parent, canonical, src, targetDir string, depth int) {
    targetRel, _ := filepath.Rel(parent, targetDir)
    meta := types.Metadata{Title: filepath.Base(targetDir), ID: canonicalID(canonical), SourceURL: src, Depth: depth, Type: "redirect", RedirectTo: targetRel}
    writeMetadata(filepath.Join(parent, filepath.Base(targetDir)+"-redirect"), meta)
}

func writeMetadata(dir string, m types.Metadata) {
    m.CrawledAt = time.Now().UTC()
    must(os.MkdirAll(dir, 0o755))
    b, _ := json.MarshalIndent(m, "", "  ")
    must(os.WriteFile(filepath.Join(dir, "metadata.json"), b, 0o644))
}

func canonicalID(canonical string) string { return strings.SplitN(canonical, ":", 2)[1] }

// -------------------- small crawler utils ----------------------------------

func httpGet(ctx context.Context, c *http.Client, u string) (*http.Response, error) {
    req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
    resp, err := c.Do(req)
    if err != nil {
        return nil, err
    }
    if resp.StatusCode != http.StatusOK {
        resp.Body.Close()
        return nil, fmt.Errorf("GET %s: %s", u, resp.Status)
    }
    return resp, nil
}

func fetchSheetTitle(ctx context.Context, c *http.Client, id string) (string, error) {
    u := fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/preview", id)
    resp, err := httpGet(ctx, c, u)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()
    root, err := html.Parse(resp.Body)
    if err != nil {
        return "", err
    }
    return extractHTMLTitle(root), nil
}

func extractHTMLTitle(root *html.Node) string {
    var title string
    var dfs func(*html.Node)
    dfs = func(n *html.Node) {
        if title != "" {
            return
        }
        if n.Type == html.ElementNode && n.Data == "title" && n.FirstChild != nil {
            title = n.FirstChild.Data
            return
        }
        for c := n.FirstChild; c != nil; c = c.NextSibling {
            dfs(c)
        }
    }
    dfs(root)
    title = titleTrimRE.ReplaceAllString(title, "")
    return strings.TrimSpace(title)
}

func firstHrefText(root *html.Node) string {
    var txt string
    var dfs func(*html.Node)
    dfs = func(n *html.Node) {
        if txt != "" {
            return
        }
        if n.Type == html.ElementNode && n.Data == "a" && n.FirstChild != nil {
            txt = nodeText(n)
            return
        }
        for c := n.FirstChild; c != nil; c = c.NextSibling {
            dfs(c)
        }
    }
    dfs(root)
    return strings.TrimSpace(txt)
}

func nodeText(n *html.Node) string {
    var b strings.Builder
    var walk func(*html.Node)
    walk = func(n *html.Node) {
        if n.Type == html.TextNode {
            b.WriteString(n.Data)
        }
        for c := n.FirstChild; c != nil; c = c.NextSibling {
            walk(c)
        }
    }
    walk(n)
    return b.String()
}

func makeSlug(title, id string) string {
    s := strings.ToLower(title)
    s = nonAlphaNum.ReplaceAllString(s, "-")
    s = multiHyphen.ReplaceAllString(s, "-")
    s = strings.Trim(s, "-")
    if len(s) > 60 {
        s = s[:60]
    }
    if s == "" {
        sum := sha1.Sum([]byte(id))
        s = fmt.Sprintf("%x", sum[:6])
    }
    return fmt.Sprintf("%s-%s", s, id[:6])
}



func resolve(base, href string) string {
    u, err := url.Parse(href)
    if err != nil || u.IsAbs() {
        return canonicalLink(href)
    }
    b, _ := url.Parse(base)
    return canonicalLink(b.ResolveReference(u).String())
}

func canonicalKey(link string) string {
    link = canonicalLink(link)
    if docRe.MatchString(link) {
        return "doc:" + docRe.FindStringSubmatch(link)[1]
    }
    if sheetRe.MatchString(link) {
        return "sheet:" + sheetRe.FindStringSubmatch(link)[1]
    }
    return ""
}





