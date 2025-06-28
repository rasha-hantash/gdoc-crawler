package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"log"
	"mime"
	"strings"
	"google.golang.org/api/googleapi"
	"github.com/rasha-hantash/gdoc-pipeline/steps/types"
)


// ---------------------------------------------------------------------------
// UPLOADER — consumes paths channel, writes id_map.json, then closes doneUpload
// --------



func RunUploader(projectID string, driveFolder string, outDir string, in <-chan string, done chan<- struct{}) {
    defer close(done)

    ctx := context.Background()
    opts := []option.ClientOption{}
    if projectID != "" {
        opts = append(opts, option.WithQuotaProject(projectID))
    }
    drv, err := drive.NewService(ctx, opts...)
    must(err)

    parentID := ""
    if driveFolder != "" {
        parentID = ensureDriveFolder(ctx, drv, driveFolder)
    }

    idMap := map[string]string{}

    for dir := range in {
        metaPath := filepath.Join(dir, "metadata.json")
        f, err := os.Open(metaPath)
        if err != nil {
            logf("WARN uploader open: %v", err)
            continue
        }
        var m types.Metadata
        if err := json.NewDecoder(f).Decode(&m); err != nil {
            logf("WARN uploader decode: %v", err)
            f.Close()
            continue
        }
        f.Close()
        if m.Type == "redirect" {
            continue
        }

        content := map[string]string{"doc": "content.html", "sheet": "content.csv"}[m.Type]
        newID := uploadDrive(ctx, drv, filepath.Join(dir, content), m, parentID)
        if newID == "" {
            continue
        }
        idMap[fmt.Sprintf("%s:%s", m.Type, m.ID)] = newID
    }

    // persist mapping for patcher
    mapPath := filepath.Join(outDir, "id_map.json")
    if len(idMap) > 0 {
        must(os.WriteFile(mapPath, mustJSON(idMap), 0o644))
        log.Printf("uploader wrote ID map → %s", mapPath)
    }
}

func ensureDriveFolder(ctx context.Context, drv *drive.Service, name string) string {
    q := fmt.Sprintf("mimeType='application/vnd.google-apps.folder' and name='%s' and trashed=false", name)
    r, err := drv.Files.List().Q(q).Fields("files(id)").Do()
    must(err)
    if len(r.Files) > 0 {
        return r.Files[0].Id
    }
    f := &drive.File{Name: name, MimeType: "application/vnd.google-apps.folder"}
    created, err := drv.Files.Create(f).Fields("id").Do()
    must(err)
    return created.Id
}

func uploadDrive(ctx context.Context, drv *drive.Service, path string, m types.Metadata, parent string) string {
    f := &drive.File{Name: m.Title, MimeType: map[string]string{"doc": "application/vnd.google-apps.document", "sheet": "application/vnd.google-apps.spreadsheet"}[m.Type]}
    if parent != "" {
        f.Parents = []string{parent}
    }
    media, err := os.Open(path)
    if err != nil {
        logf("WARN open: %v", err)
        return ""
    }
    mimeType := mime.TypeByExtension(strings.ToLower(filepath.Ext(path)))

    resp, err := drv.Files.Create(f).Media(media, googleapi.ContentType(mimeType)).Fields("id").SupportsAllDrives(true).Do()
    if err != nil {
        logf("WARN upload: %v", err)
        return ""
    }
    logf("uploaded %-6s → %s", m.Type, resp.Id)
    return resp.Id
}
