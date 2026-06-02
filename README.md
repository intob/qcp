# qcp

qcp is a personal media archival tool for managing camera footage across a set of working drives (hot) and archive drives (cold). It follows a strict copy-then-verify pipeline: every file is hashed with BLAKE3 on the way in, and the hash is stored in a `checksums.b3` manifest alongside the footage. All subsequent commands use those manifests to verify integrity.

There is no database. State is the files and the `checksums.b3` manifests on the drives themselves — if you can read the drives, you can always recover. The tool is deliberately append-only and non-destructive: it never deletes footage, and every destructive action (organise, renumber, clean) requires confirmation.

Designed for macOS. Drive type (HDD vs SSD) and connection (USB, NVMe, SATA) are auto-detected via `diskutil` to set per-drive I/O concurrency. Drives are kept awake during long operations via `caffeinate`.

---

## Install

```sh
./install.sh
```

Builds with the current git version stamped in and installs to `$(go env GOPATH)/bin/qcp`.

---

## Config

`~/.qcp` — JSON file describing cards and drives.

```json
{
  "cards": [
    { "volume": "CFEXP",  "sub": "XDROOT/Clip" },
    { "volume": "GoPro",  "sub": "DCIM" }
  ],
  "drives": [
    { "volume": "T9",         "root": "",          "role": "hot" },
    { "volume": "T7",         "root": "",          "role": "hot" },
    { "volume": "MAC",        "path": "~/Footage", "root": "", "role": "hot", "pull": false },
    { "volume": "ARCHIVE_01", "root": "Footage",   "role": "cold", "year_from": 2024 },
    { "volume": "ARCHIVE_OLD","root": "Footage",   "role": "cold", "year_to":   2023 }
  ]
}
```

**cards**
- `volume` — prefix matched against mounted volumes. `CFEXP` matches `CFEXP_01`, `CFEXP_250_01`, etc. Each matched volume lands in its own named subfolder within the mission.
- `sub` — subdirectory on the card containing footage.

**drives**
- `volume` — resolves to `/Volumes/<volume>`. Use `path` instead for local directories (e.g. `"~/Footage"`). If both are set, `volume` is the display name and `path` is the location.
- `root` — subdirectory under which year/mission dirs are created. Empty = drive root.
- `role` — `hot` (working SSD/NVMe) or `cold` (archive HDD).
- `pull` — set `false` to exclude a drive from `-pull` (useful for internal drives with limited space).
- `year_from` / `year_to` — year range this cold drive is responsible for (both optional). `-sync`, `-replicate`, and `-check` only involve a cold drive for years within its range. Hot drives are always unbounded.

Check mounted card and drive names with `ls /Volumes/`.

---

## Mission layout

```
<drive>/<root>/<year>/<NNN>_<name>/
  <card_volume>/
    <original file paths>
  checksums.b3
```

Example with two CFexpress cards and a GoPro:

```
T9/2026/042_Altissimo_with_Anton/
  CFEXP_250_01/
    Clip0001.MXF
    Clip0002.MXF
  CFEXP_250_02/
    Clip0003.MXF
  GoPro/
    GH010042.MP4
  checksums.b3
```

Each card gets its own subfolder named after the physical volume — footage is always traceable to its source media. `checksums.b3` is a sorted text file of `blake3_hash  relative_path` entries covering every file in the mission directory.

`000_*` directories (e.g. `000_Edits`) sort to the top and are synced like any mission but cannot be addressed by mission-number commands.

---

## Commands

All commands default to the current year. Pass `-year 2025` to target a specific year, or `-year all` to operate across every year on the drives.

### Ingest

```sh
qcp -ingest "Altissimo with Anton"    # create new mission in current year
qcp -ingest "Altissimo with Anton" -y # skip confirmation
qcp -ingest "Altissimo with Anton" -year 2025

qcp -ingest 42                        # append cards to existing mission 42
qcp -ingest 42 -year 2025
```

Scans all mounted cards, copies to every mounted hot drive, verifies each file against its BLAKE3 hash, and writes `checksums.b3`. Files already present on a drive are skipped — safe to run with partially mounted drives or across multiple card batches.

### Archive

```sh
qcp -sync                             # copy missions from hot → cold drives
qcp -sync -y
qcp -sync -year 2025
qcp -sync -year all                   # sync every year

qcp -replicate                        # copy missions between cold drives
qcp -replicate -y
qcp -replicate -year all

qcp -pull 42                          # pull mission back to hot drives
qcp -pull 42 -sub CFEXP_250_01        # pull only one card's subfolder
qcp -pull 42 -year 2025
```

`-sync` copies from hot drives to cold drives — only cold drives whose `year_from`/`year_to` range covers the target year receive data. Cross-checks file manifests across hot drives before copying; conflicts are reported and skipped. Partial missions are handled: only missing files are copied, so it's safe to run again after adding files to an existing mission (e.g. edit exports).

`-replicate` copies missions between cold drives. Any mounted cold drive with the data is a valid source; only cold drives scoped for the year are destinations. Use this to populate a second cold drive from an existing one, or to catch up a drive that wasn't present during `-sync`.

`-pull` selects whichever cold drive has the most files as the source, avoiding partial copies from an incompletely synced drive.

### Verify

```sh
qcp -verify 42                        # re-verify all files in a mission
qcp -verify 42 -year 2025
qcp -verify all                       # verify every mission in current year
qcp -verify all -year all             # verify the entire archive

qcp -checksum 42                      # generate checksums.b3 for a mission
qcp -checksum 42 -year 2025
qcp -checksum all                     # generate for every mission in current year
qcp -checksum all -year all
```

`-verify` re-hashes every file listed in `checksums.b3` and checks the result. `-verify all` does the same for all missions, printing one line per mission.

`-checksum` is for missions that predate the manifest or were copied by other means. It hashes all drives, cross-checks that every drive agrees on every file, and writes `checksums.b3` only if all drives agree.

### Info

```sh
qcp -list                             # missions in current year
qcp -list -year all                   # all years, newest first

qcp -status                           # drive space + mission matrix for current year
qcp -status -year 2025

qcp -check 42                         # check a specific mission across cold drives
qcp -check 42 -year 2025
qcp -check all                        # check every mission in current year
qcp -check all -year all              # check the entire archive
```

`-list` shows missions with per-drive presence columns; missions missing from a mounted drive are highlighted. `-check` / `-check all` compare each mission against every cold drive scoped for that year and report missing or extra files. Exits 1 if any mission is incomplete.

### Organise

```sh
qcp -organise                         # group loose files into seasonal mission folders
qcp -organise -year 2025
qcp -reorganise                       # regroup already-organised missions by season
qcp -renumber                         # fix mission numbers to be sequential
qcp -init                             # sync sequence counter to what's on disk
qcp -init -year all                   # scan all years
```

`-organise` extracts shoot dates from filenames and media metadata (via `ffprobe`) and groups files into `NNN_Spring`, `NNN_Summer`, `NNN_Autumn`, `NNN_Winter` folders. `-reorganise` re-runs the same grouping over already-numbered missions. `-renumber` fixes duplicate or gapped numbers after any reorganisation. `-init` is a recovery command: scans the drives and resets the sequence counter to match what's actually on disk.

### Maintenance

```sh
qcp -clean                            # remove Synology metadata, Thumbs.db, etc.
qcp -clean -year 2025
qcp -clean -year all
qcp -clean -y
```

### Flags

```sh
-year <N|all>    year to operate on (default: current year)
-y               skip confirmation prompts
-version         print version and exit
```

---

## Typical workflow

```sh
# check drive space and what's already been ingested
qcp -status

# ingest from mounted cards
qcp -ingest "Altissimo with Anton"

# more cards arrived — append to the same mission
qcp -ingest 42

# sync new missions to cold archive drives
qcp -sync

# replicate to a second cold drive
qcp -replicate

# check all missions are complete on cold drives
qcp -check all

# periodic integrity check across the whole archive
qcp -verify all -year all
```
