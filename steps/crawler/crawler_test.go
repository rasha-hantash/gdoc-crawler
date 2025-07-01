package crawler_test

import (
	"os"
	"testing"
	"time"

	"github.com/rasha-hantash/gdoc-pipeline/steps/crawler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractTitleAndLinks(t *testing.T) {
	tests := []struct {
		name          string
		htmlPath      string
		expectedTitle string
		expectedLinks int
	}{
		{
			name:          "Basic doc with one link",
			htmlPath:      "./../testdata/doc_with_link.html",
			expectedTitle: "Sample Doc",
			expectedLinks: 1,
		},
		{
			name:          "No title fallback to link text",
			htmlPath:      "./../testdata/doc_fallback_title.html",
			expectedTitle: "Click here",
			expectedLinks: 1,
		},
		{
			name:          "No links",
			htmlPath:      "./../testdata/doc_no_links.html",
			expectedTitle: "Empty Doc",
			expectedLinks: 0,
		},
		{
			name:          "Malformed HTML",
			htmlPath:      "./../testdata/malformed.html",
			expectedTitle: "",
			expectedLinks: 0,
		},
	}

	crawlerStep := crawler.NewCrawler(1, 15*time.Second, "https://example.com/doc", "testdata", nil, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			htmlData, err := os.ReadFile(tt.htmlPath)
			require.NoError(t, err)

			links, err := crawlerStep.ExtractLinks(htmlData, "doc", "https://example.com/doc", 1)
			assert.NoError(t, err)
			assert.Len(t, links, tt.expectedLinks)
		})
	}
}

func TestCanonicalizeURL(t *testing.T) {
	tests := []struct {
		name          string
		inputURL      string
		expectedKey   string
		expectedClean string
		description   string
	}{
		// Basic Google Docs URLs
		{
			name:          "Basic Google Doc URL",
			inputURL:      "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Standard Google Doc URL with /edit suffix",
		},
		{
			name:          "Google Doc with /view suffix",
			inputURL:      "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view",
			description:   "Google Doc URL with /view suffix",
		},
		{
			name:          "Google Doc with /preview suffix",
			inputURL:      "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/preview",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/preview",
			description:   "Google Doc URL with /preview suffix",
		},
		{
			name:          "Google Doc with query parameters",
			inputURL:      "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit?usp=sharing&pli=1",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit?usp=sharing&pli=1",
			description:   "Google Doc URL with tracking query parameters",
		},
		{
			name:          "Google Doc with fragment identifier",
			inputURL:      "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#heading=h.gjdgxs",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#heading=h.gjdgxs",
			description:   "Google Doc URL with fragment identifier",
		},
		{
			name:          "Google Doc with www prefix",
			inputURL:      "https://www.docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://www.docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Google Doc URL with www prefix",
		},

		// Basic Google Sheets URLs
		{
			name:          "Basic Google Sheet URL",
			inputURL:      "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "sheet:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Standard Google Sheet URL with /edit suffix",
		},
		{
			name:          "Google Sheet with /view suffix",
			inputURL:      "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view",
			expectedKey:   "sheet:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view",
			description:   "Google Sheet URL with /view suffix",
		},
		{
			name:          "Google Sheet with query parameters",
			inputURL:      "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit?usp=sharing",
			expectedKey:   "sheet:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit?usp=sharing",
			description:   "Google Sheet URL with query parameters",
		},

		// Google redirect URLs
		{
			name:          "Single level Google redirect",
			inputURL:      "https://www.google.com/url?q=https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Google redirect URL with one level of redirection",
		},
		{
			name:          "Double level Google redirect",
			inputURL:      "https://www.google.com/url?q=https://www.google.com/url?q=https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Google redirect URL with two levels of redirection",
		},
		{
			name:          "Triple level Google redirect",
			inputURL:      "https://www.google.com/url?q=https://www.google.com/url?q=https://www.google.com/url?q=https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "sheet:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Google redirect URL with three levels of redirection (max supported)",
		},
		{
			name:          "Google redirect with URL encoding",
			inputURL:      "https://www.google.com/url?q=https%3A//docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "https://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Google redirect URL with URL-encoded target",
		},

		// Edge cases and non-Google URLs
		{
			name:          "Non-Google URL",
			inputURL:      "https://example.com/document/123",
			expectedKey:   "",
			expectedClean: "https://example.com/document/123",
			description:   "URL that is not a Google Doc or Sheet",
		},
		{
			name:          "Invalid URL",
			inputURL:      "not-a-url",
			expectedKey:   "",
			expectedClean: "not-a-url",
			description:   "Invalid URL format",
		},
		{
			name:          "Empty URL",
			inputURL:      "",
			expectedKey:   "",
			expectedClean: "",
			description:   "Empty URL string",
		},
		{
			name:          "Google redirect without q parameter",
			inputURL:      "https://www.google.com/url?other=param",
			expectedKey:   "",
			expectedClean: "https://www.google.com/url?other=param",
			description:   "Google redirect URL without the required 'q' parameter",
		},
		{
			name:          "Google redirect with empty q parameter",
			inputURL:      "https://www.google.com/url?q=",
			expectedKey:   "",
			expectedClean: "https://www.google.com/url?q=",
			description:   "Google redirect URL with empty 'q' parameter",
		},
		{
			name:          "Google redirect with non-Google target",
			inputURL:      "https://www.google.com/url?q=https://example.com/doc",
			expectedKey:   "",
			expectedClean: "https://example.com/doc",
			description:   "Google redirect URL pointing to non-Google document",
		},
		{
			name:          "HTTP Google Doc URL",
			inputURL:      "http://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			expectedKey:   "doc:1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
			expectedClean: "http://docs.google.com/document/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit",
			description:   "Google Doc URL using HTTP instead of HTTPS",
		},
		{
			name:          "Google redirect with malformed URL in q parameter",
			inputURL:      "https://www.google.com/url?q=not-a-valid-url",
			expectedKey:   "",
			expectedClean: "not-a-valid-url",
			description:   "Google redirect URL with malformed target URL",
		},
	}

	crawlerStep := crawler.NewCrawler(1, 15*time.Second, "https://example.com/doc", "testdata", nil, nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canonicalKey, cleanURL := crawlerStep.CanonicalizeURL(tt.inputURL)

			assert.Equal(t, tt.expectedKey, canonicalKey,
				"canonical key mismatch for %s: expected %q, got %q", tt.description, tt.expectedKey, canonicalKey)
			assert.Equal(t, tt.expectedClean, cleanURL,
				"clean URL mismatch for %s: expected %q, got %q", tt.description, tt.expectedClean, cleanURL)
		})
	}
}
