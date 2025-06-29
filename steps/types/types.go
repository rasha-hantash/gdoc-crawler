package types

import "time"

type Metadata struct {
	Title      string    `json:"title"`
	ID         string    `json:"id"`
	SourceURL  string    `json:"source_url"`
	Depth      int       `json:"depth"`
	Type       string    `json:"type"`
	CrawledAt  time.Time `json:"crawled_at"`
	RedirectTo string    `json:"redirect_to,omitempty"`
}

type Links struct {
	Link   string
	Depth  int
	Parent string
}
