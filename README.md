# Google Docs Pipeline

A concurrent Go application that crawls, uploads, and patches Google Docs and Sheets. This tool discovers a network of public Google Docs/Sheets starting from a given URL, downloads them, uploads copies to Google Drive, and automatically fixes internal hyperlinks to point to the new copies.

## Features

- **Concurrent Processing**: Three-stage pipeline with crawler, uploader, and patcher running concurrently
- **Smart Discovery**: Automatically discovers nested documents by following hyperlinks
- **Format Support**: Handles both Google Docs (HTML export) and Google Sheets (CSV export)
- **Link Patching**: Automatically updates internal hyperlinks in uploaded documents
- **Duplicate Detection**: Avoids re-processing duplicate documents
- **Robust Error Handling**: Built-in retry logic and timeout handling
- **Configurable Depth**: Control how deep to crawl the document graph

## How It Works

The application uses a three-stage concurrent pipeline:

1. **Crawler**: Walks the document graph starting from a public Google Doc/Sheet URL, downloads HTML/CSV exports, and discovers nested documents through hyperlinks
2. **Uploader**: Takes downloaded files and uploads them to Google Drive, building an ID mapping for link patching
3. **Patcher**: Uses the Google Docs API to rewrite internal hyperlinks in uploaded documents to point to the newly uploaded copies

## Prerequisites

- Go 1.24.4 or later
- Google Cloud Project with the following APIs enabled:
  - Google Drive API
  - Google Docs API
// TODO Correct this 
- Google Cloud credentials (Application Default Credentials or service account)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/rasha-hantash/gdoc-pipeline.git
cd gdoc-pipeline
```

2. Install dependencies:
```bash
go mod tidy
```

// TODO correct this 
3. Set up Google Cloud authentication:
```bash
# Option 1: Application Default Credentials
gcloud auth application-default login

# Option 2: Service Account (recommended for production)
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
```

## Usage

### Basic Usage

```bash
go run main.go -url "https://docs.google.com/document/d/XXXXXXXX/edit"
```

### Full Command with Options

```bash
go run main.go \
    -url "https://docs.google.com/document/d/XXXXXXXX/edit" \
    -out ./output \
    -depth 8 \
    -httptimeout 15s \
    -folder "Imported Docs" \
    -project "my-gcp-project" \
    -v
```

### Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-url` | **Required.** Public Google Doc/Sheet URL to start crawling from | - |
| `-out` | Output directory for downloaded files | `./out` |
| `-depth` | Maximum depth for nested Docs/Sheets | `5` |
| `-httptimeout` | HTTP timeout per request | `15s` |
| `-folder` | Drive folder name (created if absent) | `"Imported Docs"` |
| `-project` | GCP quota-project (optional) | - |
| `-v`, `-verbose` | Enable verbose logging | `false` |

## Output Structure

The tool creates a directory structure like this:

```
out/
├── id_map.json                    # Mapping of old IDs to new Drive IDs
├── document-title-abc123/
│   ├── content.html               # Downloaded HTML content
│   └── metadata.json              # Document metadata
├── spreadsheet-title-def456/
│   ├── content.csv                # Downloaded CSV content
│   └── metadata.json              # Sheet metadata
└── nested-doc-ghi789/
    ├── content.html
    └── metadata.json
```

## Project Structure

```
gdoc-pipeline/
├── main.go                       # Main entry point and pipeline orchestration
├── steps/
│   ├── crawler.go                # Document discovery and downloading
│   ├── uploader.go               # Google Drive upload functionality
│   ├── patcher.go                # Hyperlink patching using Docs API
│   └── types/
│       └── types.go              # Shared data structures
├── go.mod                        # Go module definition
├── go.sum                        # Dependency checksums
└── README.md                     # This file
```

## Technical Details

### Crawler
- Uses regex patterns to identify Google Docs and Sheets URLs
- Downloads HTML exports for Docs and CSV exports for Sheets
- Recursively follows hyperlinks to discover nested documents
- Implements duplicate detection using canonical URL keys
- Respects depth limits to prevent infinite crawling

### Uploader
- Creates or finds the specified Google Drive folder
- Uploads HTML files as Google Docs and CSV files as Google Sheets
- Maintains an ID mapping file for the patcher
- Uses Google Drive API v3 with proper MIME type handling

### Patcher
- Waits for upload completion before starting
- Scans uploaded documents for internal Google Docs/Sheets links
- Uses Google Docs API to batch update hyperlinks
- Implements exponential backoff for 503 errors
- Rate-limited to stay within API quotas (≤60 requests/minute)

### Concurrency
- Three goroutines communicate via channels
- Crawler sends directory paths to uploader via `paths` channel
- Uploader signals completion via `doneUpload` channel
- Patcher waits for upload completion before processing

## Error Handling

- HTTP timeouts for web requests
- Retry logic with exponential backoff for Google API 503 errors
- Graceful handling of inaccessible documents
- Comprehensive logging for debugging

## Rate Limiting

The application includes built-in rate limiting:
- Patcher adds 1.1-second delays between document updates
- Uploader uses Google's client library automatic throttling
- Exponential backoff for API errors

## Limitations

- Only works with **public** Google Docs and Sheets
- Requires documents to be accessible without authentication
- Link patching only works on uploaded Google Docs (not Sheets)
- Maximum crawl depth prevents infinite loops but may miss some documents

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions, please open an issue on the GitHub repository. 