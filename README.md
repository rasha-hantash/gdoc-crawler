# gdoc‑pipeline

A minimal Go pipeline that:

1. Crawls public Google Docs/Sheets starting from a single URL.
2. Uploads local copies to your Google Drive.
3. Rewrites internal links to point at the new copies.

All in \~450 lines of well‑commented code.

---

## Prerequisites

* **Go ≥ 1.24**
* Google Cloud project with the **Drive** *and* **Docs** APIs enabled
* **Credentials** – *pick one*:

  * **Application Default Credentials (ADC)** — run `gcloud auth application-default login` once. The Google client libraries automatically read the resulting user‑credential file in `~/.config/gcloud/`. Great for local dev.
  * **Desktop OAuth client** — in **Google Cloud Console → APIs & Services → Credentials** choose **Create credentials → OAuth client ID → Desktop app** and download the `client_secret_*.json`. Then export `GOOGLE_APPLICATION_CREDENTIALS=/path/to/client_secret_*.json`.
  * **Service‑account JSON key** — headless/CI friendly. The files will live in **the service account’s own Drive**, so share a folder with the SA or migrate them later if you need them in your personal Drive. Point `GOOGLE_APPLICATION_CREDENTIALS` at the key file.

---

## Install

```bash
git clone https://github.com/rasha-hantash/gdoc-pipeline.git
cd gdoc-pipeline && go mod tidy
```

---

## Quick start

```bash
go run main.go -url "<public‑doc‑url>" -project "<gcp‑project>"
```

### Frequently‑used flags

| Flag      | Purpose                                             | Default         |
| --------- | --------------------------------------------------- | --------------- |
| `-url`    | Root public Doc/Sheet                               | **required**    |
| `-out`    | Working directory                                   | `./out`         |
| `-depth`  | Crawl depth                                         | `5`             |
| `-folder` | Drive folder name                                   | `Imported Docs` |
| `-retry`  | Resume from step (`crawler`, `uploader`, `patcher`) | —               |

Run `go run main.go -h` for the full list.

---

## Output layout

```
out/
├── id_map.json          # old → new IDs
└── <slug>/
    ├── content.html|csv # original export
    └── metadata.json
```

---

## Code layout

```
.
├── main.go          # CLI & orchestration
├── pipeline/        # step runner
└── steps/           # crawler.go, uploader.go, patcher.go, types/
```

---

## Notes

* Crawling & uploads use anonymous HTTP; only the patcher needs Docs API access.
* The pipeline is **idempotent**—rerun safely or jump to a step with `-retry`.
* Link rewriting works for Google Docs only (Sheets aren’t patchable).

MIT‑licensed — enjoy!
