# gdoc‑pipeline

A minimal Go pipeline that:

1. Crawls public Google Docs/Sheets starting from a single URL.
2. Uploads local copies to your Google Drive.
3. Rewrites internal links to point at the new copies.

---

## Prerequisites

* **Go ≥ 1.24**
* Google Cloud project with the **Drive** *and* **Docs** APIs enabled
* **Credentials** – *pick one*:

  * **Application Default Credentials (ADC)** — run `gcloud auth application-default login` once. The Google client libraries automatically read the resulting user‑credential file in `~/.config/gcloud/`. Great for local dev.
  * **Service Account (CLI, recommended for automation/CI):**
    ```bash
    # Set your project ID
    export PROJECT_ID="your-gcp-project-id"

    # 1. Create a service account
    gcloud iam service-accounts create gdoc-pipeline --display-name="Google Doc Pipeline Service Account" --project="$PROJECT_ID"

    # 2. Grant required roles
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member="serviceAccount:gdoc-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/serviceusage.serviceUsageConsumer"
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member="serviceAccount:gdoc-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
      --role="roles/editor"

    # 3. Create and download a key
    gcloud iam service-accounts keys create gdoc-pipeline-key.json \
      --iam-account=gdoc-pipeline@$PROJECT_ID.iam.gserviceaccount.com \
      --project="$PROJECT_ID"

    # 4. Enable required APIs
    gcloud services enable drive.googleapis.com docs.googleapis.com --project="$PROJECT_ID"

    # 5. Set the environment variable before running the pipeline
    export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/gdoc-pipeline-key.json"
    ```

---

## Install

```bash
git clone https://github.com/rasha-hantash/gdoc-pipeline.git
cd gdoc-pipeline && go mod tidy
```

---

## Quick start

```bash
# If using a service account, set the environment variable first:
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/gdoc-pipeline-key.json"

go run main.go -url "<public‑doc‑url>" -project "<gcp‑project>"
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
