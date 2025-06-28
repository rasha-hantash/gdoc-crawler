package steps

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"
	"strings"
	"math"
	"math/rand"
	"fmt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/docs/v1"
	"github.com/rasha-hantash/gdoc-pipeline/steps/types"
	"net/url"
)

// ---------------------------------------------------------------------------
// PATCHER — rewrites internal hyperlinks after upload completes
// ---------------------------------------------------------------------------

func RunPatcher(outDir string, projectID string, wait <-chan struct{}) {
    <-wait // block until uploader signals completion

    mapPath := filepath.Join(outDir, "id_map.json")
    idMap := map[string]string{}
    if err := json.NewDecoder(mustOpen(mapPath)).Decode(&idMap); err != nil {
        log.Printf("patcher: no id_map.json, skipping")
        return
    }
    log.Printf("patcher loaded %d ID mappings", len(idMap))

    ctx := context.Background()
    opts := []option.ClientOption{}
    if projectID != "" {
        opts = append(opts, option.WithQuotaProject(projectID))
    }
    dsvc, err := docs.NewService(ctx, opts...)
    if err != nil {
        log.Printf("patcher: %v", err)
        return
    }

    reLink := regexp.MustCompile(`https://docs\.google\.com/(document|spreadsheets)/d/([^/?#]+)`) // find raw Doc links

    // walk every metadata.json (docs only)
    _ = filepath.WalkDir(outDir, func(p string, d os.DirEntry, _ error) error {
        if d.IsDir() || d.Name() != "metadata.json" {
            return nil
        }
        var m types.Metadata
        if err := json.NewDecoder(mustOpen(p)).Decode(&m); err != nil {
            return nil
        }
        if m.Type != "doc" {
            return nil
        }

        dir := filepath.Dir(p)
        newDocID := idMap["doc:"+m.ID]
        if newDocID == "" {
            return nil
        }

        urlMap := buildURLMap(filepath.Join(dir, "content.html"), reLink, idMap)
        if len(urlMap) == 0 {
            return nil
        }
        patchOneDoc(ctx, dsvc, newDocID, urlMap)
        logf("patched %-40s (%d links)", m.Title, len(urlMap))
        time.Sleep(1100 * time.Millisecond) // stay ≤ 60 req/min
        return nil
    })

    log.Println("patcher done")
}

func buildURLMap(htmlPath string, re *regexp.Regexp, idMap map[string]string) map[string]string {
    data, _ := os.ReadFile(htmlPath)
    matches := re.FindAllSubmatch(data, -1)
    urlMap := map[string]string{}
    for _, m := range matches {
        kind := string(m[1]) // document | spreadsheets
        oldID := string(m[2])
        oldKey := map[string]string{"document": "doc:" + oldID, "spreadsheets": "sheet:" + oldID}[kind]
        newID, ok := idMap[oldKey]
        if !ok {
            continue
        }
        oldURL := stripQuery(string(m[0]))
        newURL := fmt.Sprintf("https://docs.google.com/%s/d/%s/edit", kind, newID)
        urlMap[oldURL] = newURL
    }
    return urlMap
}

func patchOneDoc(ctx context.Context, dsvc *docs.Service, docID string, urlMap map[string]string) {
    doc, err := dsvc.Documents.Get(docID).Do()
    if err != nil {
        logf("patch fetch: %v", err)
        return
    }

    var reqs []*docs.Request
    for _, se := range doc.Body.Content {
        p := se.Paragraph
        if p == nil {
            continue
        }
        for _, pe := range p.Elements {
            tr := pe.TextRun
            if tr == nil || tr.TextStyle == nil || tr.TextStyle.Link == nil {
                continue
            }
            oldURL := canonicalLink(tr.TextStyle.Link.Url)
            newURL, ok := urlMap[oldURL]
            if !ok {
                continue
            }
            reqs = append(reqs, &docs.Request{
                UpdateTextStyle: &docs.UpdateTextStyleRequest{
                    Range: &docs.Range{StartIndex: pe.StartIndex, EndIndex: pe.EndIndex},
                    TextStyle: &docs.TextStyle{Link: &docs.Link{Url: newURL}},
                    Fields: "link",
                },
            })
        }
    }
    if len(reqs) == 0 {
        return
    }

    _, err = retry(func() (*docs.BatchUpdateDocumentResponse, error) {
        return dsvc.Documents.BatchUpdate(docID, &docs.BatchUpdateDocumentRequest{Requests: reqs}).Do()
    })
    if err != nil {
        logf("patch update: %v", err)
    }
}

// retry helper with exponential back‑off on 503 backendError
func retry[T any](fn func() (T, error)) (T, error) {
    const maxAttempts = 6
    const base = time.Second
    var zero T
    for i := 0; i < maxAttempts; i++ {
        v, err := fn()
        if err == nil {
            return v, nil
        }
        if g, ok := err.(*googleapi.Error); !ok || g.Code != 503 {
            return zero, err
        }
        d := base * time.Duration(math.Pow(2, float64(i)))
        d += time.Duration(rand.Int63n(int64(d / 2)))
        time.Sleep(d)
    }
    return zero, fmt.Errorf("after %d attempts, still failing with 503", maxAttempts)
}

// misc helpers --------------------------------------------------------------

func stripQuery(u string) string {
    if i := strings.IndexAny(u, "?#"); i != -1 {
        return u[:i]
    }
    return u
}

func must(err error) {
    if err != nil {
        log.Fatal(err)
    }
}

func mustOpen(p string) *os.File {
    f, err := os.Open(p)
    must(err)
    return f
}

func mustJSON(v any) []byte {
    b, err := json.MarshalIndent(v, "", "  ")
    must(err)
    return b
}


func canonicalLink(raw string) string {
    u := raw
    for i := 0; i < 3 && redirectRe.MatchString(u); i++ {
        parsed, _ := url.Parse(u)
        q := parsed.Query().Get("q")
        if q == "" {
            break
        }
        u, _ = url.QueryUnescape(q)
    }
    return u
}