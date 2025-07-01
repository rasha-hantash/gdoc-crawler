package uploader

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/rasha-hantash/gdoc-pipeline/steps/types"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// UploadStats tracks upload statistics
type UploadStats struct {
	TotalUploaded int
	Failed        int
	Skipped       int
}

// Uploader handles uploading crawled files to Google Drive
type Uploader struct {
	driveService *drive.Service
	projectID    string
	driveFolder  string
	outDir       string
	// MIME type mappings for different file types
	mimeTypes map[string]string
}

// NewUploader creates a new uploader with the given configuration
func NewUploader(ctx context.Context, projectID string, driveFolder string, outDir string) (*Uploader, error) {
	opts := []option.ClientOption{}
	if projectID != "" {
		opts = append(opts, option.WithQuotaProject(projectID))
	}

	drv, err := drive.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Drive service: %w", err)
	}

	return &Uploader{
		driveService: drv,
		projectID:    projectID,
		driveFolder:  driveFolder,
		outDir:       outDir,
		mimeTypes: map[string]string{
			"doc":   "application/vnd.google-apps.document",
			"sheet": "application/vnd.google-apps.spreadsheet",
		},
	}, nil
}

// Name implements the Step interface
func (u *Uploader) Name() string {
	return "uploader"
}

// Run implements the Step interface and starts the upload process
func (u *Uploader) Run(ctx context.Context) error {
	parentID, err := u.createDriveFolder(ctx)
	if err != nil {
		return fmt.Errorf("creating Drive folder: %w", err)
	}

	// Discover directories to process by scanning output directory
	dirs, err := u.discoverDirectories()
	if err != nil {
		return fmt.Errorf("discovering directories: %w", err)
	}

	idMap := make(map[string]string)
	stats := &UploadStats{}

	slog.Info("starting upload",
		slog.String("output_dir", u.outDir),
		slog.Int("directories_found", len(dirs)))

	for _, dir := range dirs {
		metadata, err := u.loadMetadata(dir)
		if err != nil {
			return fmt.Errorf("loading metadata from %s: %w", dir, err)
		}

		if metadata.IsRedirect {
			stats.Skipped++
			continue
		}

		if err := u.processDirectory(ctx, dir, parentID, idMap, metadata); err != nil {
			slog.Warn("processing directory failed",
				slog.String("dir", dir),
				slog.Any("error", err))
			stats.Failed++
			continue
		}
		stats.TotalUploaded++
	}

	if err := u.writeIDMap(u.outDir, idMap); err != nil {
		return fmt.Errorf("writing ID map: %w", err)
	}

	slog.Info("upload completed",
		slog.Int("uploaded", stats.TotalUploaded),
		slog.Int("failed", stats.Failed),
		slog.Int("skipped", stats.Skipped))
	return nil
}

// discoverDirectories recursively scans the output directory for subdirectories with metadata
func (u *Uploader) discoverDirectories() ([]string, error) {
	var dirs []string

	err := filepath.WalkDir(u.outDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip if not a directory
		if !d.IsDir() {
			return nil
		}

		// Check if this directory contains metadata.json
		metadataPath := filepath.Join(path, "metadata.json")
		if _, err := os.Stat(metadataPath); err == nil {
			dirs = append(dirs, path)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("walking output directory: %w", err)
	}

	slog.Info("discovered directories", slog.Int("count", len(dirs)))
	return dirs, nil
}

// processDirectory handles uploading a single directory
func (u *Uploader) processDirectory(ctx context.Context, dir string, parentID string, idMap map[string]string, metadata *types.Metadata) error {
	contentFile := u.getContentFileName(metadata.Type)
	if contentFile == "" {
		return fmt.Errorf("unsupported content type: %s", metadata.Type)
	}

	filePath := filepath.Join(dir, contentFile)
	newID, err := u.uploadFile(ctx, filePath, metadata, parentID)
	if err != nil {
		return fmt.Errorf("uploading file: %w", err)
	}

	key := fmt.Sprintf("%s:%s", metadata.Type, metadata.ID)
	idMap[key] = newID

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

// createDriveFolder creates a new Drive folder and returns its ID
func (u *Uploader) createDriveFolder(ctx context.Context) (string, error) {
	if u.driveFolder == "" {
		return "", nil // No parent folder
	}

	// Search for existing folder
	q := fmt.Sprintf("mimeType='application/vnd.google-apps.folder' and name='%s' and trashed=false",
		u.driveFolder)

	r, err := u.driveService.Files.List().Q(q).Fields("files(id)").Do()
	if err != nil {
		return "", fmt.Errorf("searching for folder: %w", err)
	}

	if len(r.Files) > 0 {
		slog.Info("found existing drive folder",
			slog.String("name", u.driveFolder),
			slog.String("id", r.Files[0].Id))
		return r.Files[0].Id, nil
	}

	// Create new folder
	f := &drive.File{
		Name:     u.driveFolder,
		MimeType: "application/vnd.google-apps.folder",
	}

	created, err := u.driveService.Files.Create(f).Fields("id").Do()
	if err != nil {
		return "", fmt.Errorf("creating folder: %w", err)
	}

	slog.Info("created drive folder",
		slog.String("name", u.driveFolder),
		slog.String("id", created.Id))
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

	slog.Info("uploaded file",
		slog.String("type", metadata.Type),
		slog.String("id", resp.Id),
		slog.String("title", metadata.Title))
	return resp.Id, nil
}

// writeIDMap writes the ID mapping to a JSON file
func (u *Uploader) writeIDMap(outDir string, idMap map[string]string) error {
	if len(idMap) == 0 {
		slog.Info("no files uploaded, skipping ID map creation")
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

	slog.Info("wrote ID map",
		slog.String("path", mapPath),
		slog.Int("mappings", len(idMap)))
	return nil
}
