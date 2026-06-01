# qcp

Fast, verified media ingest and archiving for camera cards. Copies footage from mounted cards to one or more drives, hashes every file with Blake3, and writes a `checksums.b3` manifest per mission. Subsequent commands use those manifests to verify integrity across drives.

Designed for macOS. Drive type (HDD/SSD) is auto-detected via `diskutil` to set optimal I/O concurrency per destination.

---

## Build

```
go build -o qcp .
```

Move the binary somewhere on your `$PATH`, e.g. `/usr/local/bin/qcp`.

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
    { "volume": "T9",       "root": "",        "role": "hot" },
    { "volume": "T7",       "root": "",        "role": "hot" },
    { "volume": "Footage",  "path": "~/Footage", "root": "", "role": "hot" },
    { "volume": "ARCHIVE_01", "root": "Footage", "role": "cold" }
  ]
}
```

- **cards** — card volumes to ingest from. `volume` is a prefix — any mounted volume whose name starts with it will be picked up. `sub` is the subdirectory on the card containing footage.
- **drives** — destination drives. `volume` resolves to `/Volumes/<volume>` on macOS. Use `path` instead (e.g. `"~/Footage"`) for local directories not under `/Volumes`. If both are set, `volume` is used as the display name and `path` as the actual location. `root` is a subdirectory under which year/mission dirs are created (empty = drive root). `role` is `hot` (working SSD) or `cold` (archive HDD).

Card `volume` values are prefix-matched against mounted volumes. A single entry `"CFEXP"` will match `CFEXP_01`, `CFEXP_02`, `CFEXP_250_01`, etc. — each landing in its own named subfolder. Check mounted names with `ls /Volumes/`.

---

## Mission layout

Each ingest creates a numbered mission directory:

```
<drive>/<root>/<year>/<NNN>_<slug>/
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

Each card gets its own subfolder named after the physical volume, so footage is always traceable back to the source media. `checksums.b3` is a sorted text file of `blake3_hash  relative_path` entries, one per file.

### Naming convention

Name your cards consistently and descriptively — the volume name becomes the subfolder name in every mission archive. A scheme like `CFEXP_250_01` (type, size in GB, card number) makes archives self-documenting without needing a separate log.

`000_*` directories (e.g. `000_Edits`) sort to the top and are synced like any mission, but cannot be addressed by number-based commands.

---

## Commands

### Ingest from cards

```
qcp -mission "Altissimo with Anton"
```

Scans all mounted cards, copies all files to every mounted drive, verifies each file against its Blake3 hash, and writes `checksums.b3`. Files already present at the destination are skipped.

Flags:
- `-year <N>` — override year (default: current year)
- `-y` — skip confirmation prompt

### Append to existing mission

```
qcp -to <N>
```

Adds files from currently mounted cards to an existing mission. Files already present at the destination are skipped, so you can safely run this with multiple cards mounted at different times.

```
qcp -to 42
qcp -to 42 -year 2025
```

### Sync hot → cold

```
qcp -sync
```

Finds missions present on any hot drive but missing from cold drives, and copies them across. Missions found on multiple hot drives are cross-checked by file manifest before syncing — conflicts are reported and skipped.

### Update existing mission on cold drives

```
qcp -update <N>
```

Copies files present on the hot drive but missing from cold drives for a specific mission. Useful for syncing new files added after the initial ingest (e.g. edit exports).

```
qcp -update 42
qcp -update 42 -year 2025
```

### Verify

```
qcp -verify <N>
```

Re-hashes every file in a mission across all mounted drives and checks each against `checksums.b3`. Reports any failures.

```
qcp -verify 42
qcp -verify 42 -year 2025
```

### Generate checksums for existing missions

```
qcp -checksum <N>
```

For missions that pre-date `checksums.b3` (or were copied by other means): hashes all files on every mounted copy of the mission, cross-checks them against each other, and writes `checksums.b3` to all drives only if they all agree. Skips drives that already have a manifest.

```
qcp -checksum 42
qcp -checksum 42 -year 2025
```

---

## Typical workflow

```sh
# cards in — ingest to all mounted drives
qcp -mission "Altissimo with Anton"

# later, sync to cold archive HDDs
qcp -sync

# add edit exports to an existing mission
qcp -update 42

# periodic integrity check
qcp -verify 42
```

---

## Notes

- Dotfiles (`.DS_Store`, `.Spotlight-V100`, etc.) are always skipped.
- Drive type is detected via `diskutil info` — SSD gets 4 concurrent workers, HDD gets 1 (sequential I/O).
- Interrupting a sync or ingest with `^C` prompts to clean up partial directories.
