# gdoc‑pipeline

A minimal Go pipeline that:

1. Crawls public Google Docs/Sheets starting from a single URL.
2. Uploads local copies to your Google Drive.
3. Rewrites internal links to point at the new copies.

---

## Prerequisites

* **Go ≥ 1.24**
* Google Cloud project with the **Drive** *and* **Docs** APIs enabled
* **Credentials** –:
## 0) Pick or create a Google Cloud project that has the Drive API enabled.
a. **Pick or create** a Google Cloud project that has the **Google Drive API** enabled.  Quick guide&nbsp;▸ <https://developers.google.com/workspace/guides/create-project>

b. Point **gcloud** at that project and enable the API:

   ```bash
   gcloud config set project YOUR_PROJECT_ID
   gcloud services enable drive.googleapis.com
   ```


## 1) Create an OAuth “Desktop” client and download client_secret.json  (Credentials → Create credentials → OAuth client ID → Desktop app).

## 2) Login, *requesting the Drive scope*, and tell gcloud to use your client ID.
gcloud auth application-default login \
  --client-id-file=client_secret_XXXXXX.json \
  --scopes=https://www.googleapis.com/auth/drive.file


---

## Install

```bash
git clone https://github.com/rasha-hantash/gdoc-pipeline.git
cd gdoc-pipeline && go mod tidy
```

---

## Quick start

```bash

go run main.go -url "<public‑doc‑url>"
```

## Retry a step 
```bash 
go run main.go -url "<public‑doc‑url>"  -retry "uploader"
```


### Frequently‑used flags

| Flag      | Purpose                                             | Default         |
| --------- | --------------------------------------------------- | --------------- |
| `-url`    | Root public Doc/Sheet                               | **required**    |
| `-out`    | Working directory                                   | `./out`         |
| `-depth`  | Crawl depth                                         | `5`             |
| `-folder` | Drive folder name                                   | `Imported Docs` |
| `-retry`  | Resume from step (`crawler`, `uploader`, `patcher`) | —               |

Run `go run main.go -h` for the full list.

---

## Output layout

```
out/
├── id_map.json          # old → new IDs
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
* Link rewriting works for Google Docs only (Sheets aren't patchable).

MIT‑licensed — enjoy!
