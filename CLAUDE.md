# haver-data

## Commit message style

Short, imperative subject line — no period, no co-author trailer. Examples from this repo:

- `Add 122 daily financial market series (Ken)`
- `Fix quarantine gaps and formatting-destructive save_config in pull/manage`
- `Update manage.py`

Do not add `Co-Authored-By:` lines. Keep it to one line unless the change genuinely needs a body.

## Series management

All additions and removals of series must go through `src/manage.py`, not by editing `config/series.yaml` directly.

- **Add a single series:** `python src/manage.py add CODE FREQ --tags tag1 tag2 ...`
- **Remove a series:** `python src/manage.py remove CODE`
- **List tracked series:** `python src/manage.py list`
- **Bulk add (many series at once):** write a short Python script that imports and calls `_append_series` from `src/manage.py` in a loop — do not hand-edit the YAML

`manage.py` handles duplicate detection and quarantine checks automatically. Bypassing it risks adding duplicates or re-adding quarantined series.
