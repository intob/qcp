# qcp

Fast, verified media ingest and archiving for camera cards. Copies footage from mounted cards to one or more drives, hashes every file with BLAKE3, and writes a `checksums.b3` manifest per mission. All subsequent commands use those manifests to verify integrity across drives.

Designed for macOS. Drive type (HDD/SSD) is auto-detected via `diskutil` to set optimal I/O concurrency. Drives are kept awake during long operations.

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
- `year_from` / `year_to` — year range this cold drive is responsible for (both optional, 0 = unbounded). `-sync`, `-replicate`, and `-check` only involve a cold drive for years within its range. Hot drives are always unbounded.

Check mounted card names with `ls /Volumes/`.

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

Each card gets its own subfolder named after the physical volume — footage is always traceable to source media. `checksums.b3` is a sorted text file of `blake3_hash  relative_path` entries.

`000_*` directories (e.g. `000_Edits`) sort to the top and are synced like any mission, but cannot be addressed by number-based commands.

---

## Commands

### Ingest

```sh
qcp -ingest "Altissimo with Anton"           # create new mission
qcp -ingest "Altissimo with Anton" -year 2025
qcp -ingest "Altissimo with Anton" -y        # skip confirmation

qcp -ingest 42                               # append cards to existing mission 42
qcp -ingest 42 -year 2025
```

Scans all mounted cards, copies to every mounted drive, verifies each file against its BLAKE3 hash, and writes `checksums.b3`. Files already present are skipped — safe to run with partially mounted drives or multiple card batches.

### Archive

```sh
qcp -sync           # copy missions from hot drives to cold drives
qcp -sync -y
qcp -sync -year 2025

qcp -replicate      # copy missions between cold drives
qcp -replicate -y
qcp -replicate -year 2025

qcp -pull 42                        # pull mission back to hot drives
qcp -pull 42 -sub CFEXP_250_01      # pull only one card's subfolder
qcp -pull 42 -year 2025
```

`-sync` copies from hot drives to cold drives — only cold drives scoped for the given year receive data. Cross-checks file manifests across hot drives before copying; conflicts are reported and skipped. Partial missions are handled too, so running it after adding files to an existing mission (e.g. edit exports) will copy only what's missing.

`-replicate` copies missions between cold drives. Any mounted cold drive with the data is a valid source; only cold drives scoped for the year are destinations. Follows the same copy-then-verify pipeline as `-sync`. Use this to populate a second cold drive from an existing one, or to catch up a cold drive that wasn't present during the original `-sync`.

### Verify

```sh
qcp -verify 42              # re-verify all files in a mission
qcp -verify 42 -year 2025

qcp -checksum 42            # generate checksums.b3 for an existing mission
qcp -checksum-all           # generate checksums.b3 for every mission in the year
```

`-verify` re-hashes every file and checks against `checksums.b3`. Use `-checksum` / `-checksum-all` for missions that predate the manifest or were copied by other means — hashes all drives, cross-checks them, and writes `checksums.b3` only if all drives agree.

### Info

```sh
qcp -list               # all years, newest first
qcp -list -year 2026    # single year, per-mission drive presence

qcp -status             # drive space + mission matrix for current year
qcp -status -year 2025

qcp -check 42           # check a specific mission across cold drives
qcp -check 42 -year 2025
qcp -check-all          # check every mission across all years
qcp -check-all -year 2025
```

`-list` shows every mission grouped by year with drive presence columns — missions missing from a mounted drive are highlighted. `-check` / `-check-all` compare each mission against every cold drive scoped for that year and report missing or unexpected files. Only cold drives whose year range covers the mission's year are included. Both commands exit 1 if any mission is incomplete.

### Organise

```sh
qcp -organise           # group loose files into seasonal mission folders
qcp -reorganise         # regroup already-organised missions by season
qcp -renumber           # fix mission numbers to be sequential
qcp -init               # scan drives and initialise missing sequence numbers
```

`-organise` extracts shoot dates from filenames and media metadata (via `ffprobe`) and groups files into `NNN_Spring`, `NNN_Summer`, `NNN_Autumn`, `NNN_Winter` folders. `-reorganise` re-runs the same grouping over already-numbered missions. `-renumber` fixes duplicate or gapped numbers after any reorganisation.

### Maintenance

```sh
qcp -clean              # remove Synology metadata, Thumbs.db, etc.
qcp -clean -year 2026   # limit to a specific year
qcp -clean -y
```

### Flags

```sh
-year <N>    year override (default: current year)
-y           skip confirmation prompts
-version     print version and exit
```

---

## Typical workflow

```sh
# check drive space and mission status
qcp -status

# ingest from mounted cards
qcp -ingest "Altissimo with Anton"

# more cards arrived — append to the same mission
qcp -ingest 42

# sync everything to cold archive
qcp -sync

# replicate to a second cold drive
qcp -replicate

# check all missions are complete on cold drives
qcp -check-all

# periodic integrity verification
qcp -verify 42
```
