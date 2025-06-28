package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/rasha-hantash/gdoc-pipeline/steps/types"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// UploaderConfig holds the uploader configuration
type UploaderConfig struct {
	ProjectID   string
	DriveFolder string
	Verbose     bool
}

// DefaultUploaderConfig returns a default uploader configuration
func DefaultUploaderConfig() UploaderConfig {
	return UploaderConfig{
		DriveFolder: "Imported Docs",
		Verbose:     true,
	}
}

// Uploader handles uploading crawled files to Google Drive
type Uploader struct {
	driveService *drive.Service
	config       UploaderConfig

	// MIME type mappings for different file types
	mimeTypes map[string]string
}

// NewUploader creates a new uploader with the given configuration
func NewUploader(ctx context.Context, config UploaderConfig) (*Uploader, error) {
	opts := []option.ClientOption{}
	if config.ProjectID != "" {
		opts = append(opts, option.WithQuotaProject(config.ProjectID))
	}

	drv, err := drive.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Drive service: %w", err)
	}

	return &Uploader{
		driveService: drv,
		config:       config,
		mimeTypes: map[string]string{
			"doc":   "application/vnd.google-apps.document",
			"sheet": "application/vnd.google-apps.spreadsheet",
		},
	}, nil
}

// UploadStats tracks upload statistics
type UploadStats struct {
	TotalUploaded int
	Failed        int
	Skipped       int
}

// Run starts the upload process
func (u *Uploader) Run(ctx context.Context, outDir string, in <-chan string, done chan<- struct{}) error {
	defer close(done)

	parentID, err := u.ensureDriveFolder(ctx)
	if err != nil {
		return fmt.Errorf("ensuring Drive folder: %w", err)
	}

	idMap := make(map[string]string)
	stats := &UploadStats{}

	for dir := range in {
		if err := u.processDirectory(ctx, dir, parentID, idMap, stats); err != nil {
			u.logf("WARN processing directory %s: %v", dir, err)
			stats.Failed++
		}
	}

	if err := u.writeIDMap(outDir, idMap); err != nil {
		return fmt.Errorf("writing ID map: %w", err)
	}

	u.logf("Upload completed: %d uploaded, %d failed, %d skipped",
		stats.TotalUploaded, stats.Failed, stats.Skipped)
	return nil
}

// processDirectory handles uploading a single directory
func (u *Uploader) processDirectory(ctx context.Context, dir string, parentID string, idMap map[string]string, stats *UploadStats) error {
	metadata, err := u.loadMetadata(dir)
	if err != nil {
		return fmt.Errorf("loading metadata: %w", err)
	}

	if metadata.Type == "redirect" {
		stats.Skipped++
		return nil
	}

	contentFile := u.getContentFileName(metadata.Type)
	if contentFile == "" {
		return fmt.Errorf("unsupported content type: %s", metadata.Type)
	}

	filePath := filepath.Join(dir, contentFile)
	newID, err := u.uploadFile(ctx, filePath, metadata, parentID)
	if err != nil {
		return fmt.Errorf("uploading file: %w", err)
	}

	if newID != "" {
		key := fmt.Sprintf("%s:%s", metadata.Type, metadata.ID)
		idMap[key] = newID
		stats.TotalUploaded++
	} else {
		stats.Failed++
	}

	return nil
}

// loadMetadata loads metadata from a directory
func (u *Uploader) loadMetadata(dir string) (*types.Metadata, error) {
	metaPath := filepath.Join(dir, "metadata.json")
	f, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var metadata types.Metadata
	if err := json.NewDecoder(f).Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// getContentFileName returns the content file name for a given type
func (u *Uploader) getContentFileName(fileType string) string {
	contentFiles := map[string]string{
		"doc":   "content.html",
		"sheet": "content.csv",
	}
	return contentFiles[fileType]
}

// ensureDriveFolder ensures the Drive folder exists and returns its ID
func (u *Uploader) ensureDriveFolder(ctx context.Context) (string, error) {
	if u.config.DriveFolder == "" {
		return "", nil // No parent folder
	}

	// Search for existing folder
	q := fmt.Sprintf("mimeType='application/vnd.google-apps.folder' and name='%s' and trashed=false",
		u.config.DriveFolder)

	r, err := u.driveService.Files.List().Q(q).Fields("files(id)").Do()
	if err != nil {
		return "", fmt.Errorf("searching for folder: %w", err)
	}

	if len(r.Files) > 0 {
		u.logf("Found existing folder: %s (ID: %s)", u.config.DriveFolder, r.Files[0].Id)
		return r.Files[0].Id, nil
	}

	// Create new folder
	f := &drive.File{
		Name:     u.config.DriveFolder,
		MimeType: "application/vnd.google-apps.folder",
	}

	created, err := u.driveService.Files.Create(f).Fields("id").Do()
	if err != nil {
		return "", fmt.Errorf("creating folder: %w", err)
	}

	u.logf("Created Drive folder: %s (ID: %s)", u.config.DriveFolder, created.Id)
	return created.Id, nil
}

// uploadFile uploads a single file to Google Drive
func (u *Uploader) uploadFile(ctx context.Context, filePath string, metadata *types.Metadata, parentID string) (string, error) {
	mimeType, ok := u.mimeTypes[metadata.Type]
	if !ok {
		return "", fmt.Errorf("unsupported file type: %s", metadata.Type)
	}

	// Prepare Drive file metadata
	driveFile := &drive.File{
		Name:     metadata.Title,
		MimeType: mimeType,
	}

	if parentID != "" {
		driveFile.Parents = []string{parentID}
	}

	// Open the content file
	media, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("opening file: %w", err)
	}
	defer media.Close()

	// Determine media MIME type
	mediaMimeType := mime.TypeByExtension(strings.ToLower(filepath.Ext(filePath)))

	// Upload the file
	resp, err := u.driveService.Files.Create(driveFile).
		Media(media, googleapi.ContentType(mediaMimeType)).
		Fields("id").
		SupportsAllDrives(true).
		Do()

	if err != nil {
		return "", fmt.Errorf("Drive API upload: %w", err)
	}

	u.logf("uploaded %-6s → %s (title: %s)", metadata.Type, resp.Id, metadata.Title)
	return resp.Id, nil
}

// writeIDMap writes the ID mapping to a JSON file
func (u *Uploader) writeIDMap(outDir string, idMap map[string]string) error {
	if len(idMap) == 0 {
		u.logf("No files uploaded, skipping ID map creation")
		return nil
	}

	mapPath := filepath.Join(outDir, "id_map.json")
	data, err := json.MarshalIndent(idMap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling ID map: %w", err)
	}

	if err := os.WriteFile(mapPath, data, 0o644); err != nil {
		return fmt.Errorf("writing ID map file: %w", err)
	}

	u.logf("uploader wrote ID map → %s (%d mappings)", mapPath, len(idMap))
	return nil
}

// logf logs a message if verbose logging is enabled
func (u *Uploader) logf(format string, v ...any) {
	if u.config.Verbose {
		log.Printf(format, v...)
	}
}

// RunUploader provides backward compatibility with the old API
func RunUploader(projectID string, driveFolder string, outDir string, in <-chan string, done chan<- struct{}) {
	ctx := context.Background()

	config := UploaderConfig{
		ProjectID:   projectID,
		DriveFolder: driveFolder,
		Verbose:     true,
	}

	uploader, err := NewUploader(ctx, config)
	if err != nil {
		logf("FATAL: failed to create uploader: %v", err)
		close(done)
		return
	}

	if err := uploader.Run(ctx, outDir, in, done); err != nil {
		logf("FATAL: uploader failed: %v", err)
	}
}
