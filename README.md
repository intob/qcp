# qcp

qcp is a personal media archival tool for managing camera footage across a set of working drives (hot) and archive drives (cold). It follows a strict copy-then-verify pipeline: every file is hashed with BLAKE3 on the way in, and the hash is stored in a `checksums.b3` manifest alongside the footage. All subsequent commands use those manifests to verify integrity.

Derived media lives alongside but apart from the footage: `qcp -proxy` builds a 1080p browse tier and stills under a `proxies/` root, and `qcp -index` turns those into a static site you can browse with nothing plugged in. Proxies are never archived — they are regenerable, and cold space is the scarce resource.

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
  ],
  "look": "/Library/Application Support/Blackmagic Design/DaVinci Resolve/LUT/My Look.cube"
}
```

**look**
- Optional path to a creative `.cube` baked into the browse tier in place of the
  generated technical conversion. It must take **S-Log3 in and deliver finished
  Rec.709 out**, because nothing is applied after it — a look replaces the whole
  chain (gamut matrix, tone-map and gamma encode), it does not layer on top of
  one. Only log clips get it; Rec.709 sources have no log to give the cube and
  pass through untouched. Omit it for the ungraded technical conversion.

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
qcp -ingest "Altissimo" -proxy=false  # skip proxy generation
```

Scans all mounted cards, copies to every mounted hot drive, verifies each file against its BLAKE3 hash, and writes `checksums.b3`. Files already present on a drive are skipped — safe to run with partially mounted drives or across multiple card batches.

Every copy — here, and in `-sync`, `-replicate` and `-pull` — is written under a hidden `.qcp-part-` name and takes its real one only once the bytes are on the disk. A run killed mid-copy therefore leaves either nothing or a whole file at the destination, so "already present" stays a safe answer to "does this still need copying". The leftovers are invisible to every listing and are cleared by the next run that copies into that mission.

Once a mission verifies, browse-tier proxies and stills are generated for it while the cards are still mounted. Pass `-proxy=false` to skip that. The ProRes edit tier is never generated here — ask for it explicitly with `qcp -proxy <n> -tier edit`.

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
qcp -pull 42,44,50                    # pull several missions in one batch
qcp -pull 42-48                       # pull an inclusive range
qcp -pull 42-44,48 -y
qcp -pull 42 -sub CFEXP_250_01        # pull only one card's subfolder
qcp -pull 42 -year 2025

qcp -copy 42                          # copy mission to every other hot drive
qcp -copy 42 -to SSD2                 # ...or just one
qcp -copy 42-48 -to SSD2,LAPTOP       # ...or a named list

qcp -evict 42                         # free hot space, keeping the cold copy
qcp -evict 42-48 -copies 2            # require two verified cold copies
qcp -evict 42 -from LAPTOP            # only from one hot drive
```

`-sync` copies from hot drives to cold drives — only cold drives whose `year_from`/`year_to` range covers the target year receive data. Cross-checks file manifests across hot drives before copying; conflicts are reported and skipped. Partial missions are handled: only missing files are copied, so it's safe to run again after adding files to an existing mission (e.g. edit exports).

`-replicate` copies missions between cold drives. Any mounted cold drive with the data is a valid source — the one holding the most of a given mission is the one it is read from, so a drive that is itself short of a file gets it filled in rather than defining the mission; only cold drives scoped for the year are destinations. Use this to populate a second cold drive from an existing one, or to catch up a drive that wasn't present during `-sync`.

`-pull` selects whichever cold drive has the most files as the source, avoiding partial copies from an incompletely synced drive.

Missions can be given as a number, a comma-separated list, an inclusive range, or any combination. Every mission is resolved before anything is copied, so a bad number is reported up front; the rest of the batch still runs and the exit status is non-zero. Sizes and free-space warnings are totalled per hot drive across the whole batch, and only missing files are copied, so re-running a batch is cheap. `-sub` applies to a single mission only. Interrupting a pull offers to remove the mission directories that run created, leaving pre-existing ones alone.

`-copy` moves missions between hot drives. The source is whichever mounted hot drive holds the most files for that mission; destinations default to every other mounted hot drive, honouring `pull: false`. Naming drives with `-to` is explicit, so it overrides `pull: false`. Use `-sync` to reach cold drives.

`-evict` deletes missions from hot drives once the cold copy is proven good, to free space. It is the only command that removes data, so the bar is high. For each cold copy it relies on: every file on the hot copy must be present on the cold drive *and* listed in its `checksums.b3`, both copies must have a manifest, the two must agree wherever both mention a file, and every file the manifest lists is then re-read from the cold drive and hashed. Nothing is deleted unless all of that passes for every mission in the batch — a single failure aborts the whole run. `-copies N` demands that many independent cold copies clear the bar. `-quick` skips the re-read and trusts the cold manifest, which is much faster but only proves the manifests agree, not that the archived bytes are still readable.

All of `-pull`, `-copy`, `-evict`, `-verify`, `-checksum` and `-check` take the same mission selection: a single number (`42`), a comma-separated list (`42,44`), an inclusive range (`42-48`), or any combination (`42-44,48`). Ranges are capped at 500 missions. Each mission is reported separately and the run continues past failures, exiting non-zero at the end if any failed.

Copy concurrency is limited per source drive as well as per destination. Sizing the pool by the destination alone means one read stream per destination worker — and one per worker per destination when several are mounted — so a single archive HDD would serve many interleaved streams, losing far more to seeks than the parallelism gains. Every copy takes a read slot on its source drive, so an HDD source is read by one worker at a time no matter how many destinations it feeds.

### Verify

```sh
qcp -verify 42                        # re-verify all files in a mission
qcp -verify 42,44,50                  # several missions
qcp -verify 42-48                     # an inclusive range
qcp -verify 42 -year 2025
qcp -verify all                       # verify every mission in current year
qcp -verify all -year all             # verify the entire archive

qcp -checksum 42                      # generate checksums.b3 for a mission
qcp -checksum 42-48                   # several missions
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

`-check` compares the mission's content — what a transfer would carry, so each drive's own `checksums.b3` is not one of the files compared. It also compares the `checksums.b3` files between drives and reports any file whose recorded hash differs (`≠`). This is the one thing `-verify` cannot catch: it holds each drive to its own manifest, so two copies that differ but are each self-consistent both pass. Comparing the stored manifests costs nothing beyond reading them.

`-list` shows each mission's size and a column per drive: `✓` where that drive's copy is fully covered by its `checksums.b3`, `·` where the mission is present but not (or only partly) checksummed, `−` where it is absent. Sizes come from the first drive holding the mission and exclude `checksums.b3` itself, so they match across drives. Getting them means walking each mission directory, so `-list` does real work now rather than only reading directory names. `-check` / `-check all` compare each mission against every cold drive scoped for that year and report missing or extra files. Exits 1 if any mission is incomplete.

### Proxies

```sh
qcp -proxy 42                         # browse tier for one mission
qcp -proxy 42,44,50                   # several missions
qcp -proxy 42-48                      # an inclusive range
qcp -proxy all                        # every mission in the current year
qcp -proxy all -year 2025
qcp -proxy 42 -tier both              # browse + ProRes edit tier
qcp -proxy 42 -tier edit -to T7       # edit tier only, onto a named drive

qcp -index                            # build the static index in ~/qcp-index
qcp -index -to ~/Desktop/qcp-index    # ...or somewhere else

qcp -serve                            # serve that index on localhost:8080
qcp -serve -addr :8080                # ...reachable from other devices on the LAN

qcp -resolve                          # push flagged clips into the open Resolve project
qcp -resolve -unflag                  # ...and clear the flag from clips no longer flagged
```

`-proxy` generates derived renditions per source clip. The **browse tier** is
the default: 1080p H.264 at 6 Mbps with the log-to-Rec.709 transform baked in,
plus a poster frame and a 100-frame sprite sheet for hover-scrubbing. Budget up
to ~257GB for the 98-hour library, of which the stills are only ~750MB — less
in practice, because the width is a ceiling rather than a target.

**Nothing is ever upscaled.** A clip narrower than 1920 keeps its own size, and
the bitrate follows the frame down rather than staying flat: the rate scales
with the output pixel count and is never above the source's own. A 320x240 clip
from 2014 stays 320x240, and its proxy comes out about half the size of the
footage it stands in for instead of several times larger.

Resolution and bitrate move together, and the pairing is not arbitrary — 1080p
at the old 2.5 Mbps measures *worse* than 720p at 2.5 Mbps, so a resolution
bump on its own would have been a downgrade. See `PROXIES.md`.

Set `look` in `~/.qcp` to bake a creative grade into the browse tier instead of
the technical conversion. The cube is copied into `proxies/luts/` under a name
derived from its own plus a hash of its contents, so the proxy tree records
which look it was baked with and stays readable after the original moves.
Changing the look — pointing at a different cube, editing one in place, or
removing it — changes the transform recorded per clip, which marks the affected
proxies stale and rebuilds them as you touch each mission. The edit tier is
never graded, and the camera originals are never touched.

Only one `-proxy` run may work on a drive's proxy tree at a time; a second is
turned away rather than queued, because two runs over one mission overwrite each
other's manifest and end up redoing work. A run also clears any unfinished
`.qcp-part` files left behind by a previous run that was killed outright.

The **edit tier** — ProRes Proxy 1080p, ungraded, audio preserved — is opt-in
via `-tier edit` or `-tier both`. It is not generated by default: measured
concurrent decode shows the camera originals already sustain 2.4–4.2× realtime
on a four-layer mixed timeline off a hot SSD, so ProRes proxies are not needed
for normal editing and would cost about 1TB of hot space to keep standing. When
both tiers are asked for they come off a single decode, which makes the second
rendition nearly free.

The edit tier is **never** colour-transformed. If the proxy were graded and the
camera original were not, toggling proxies in Resolve would make the image jump
and you would be grading against a moving target. Set the transform at project
level instead. The browse tier *is* graded, because a log thumbnail is a flat
grey wash and tells you nothing about the shot.

The transform is chosen per clip from the Sony `*M01.XML` sidecar, which records
`CaptureGammaEquation` and `CaptureColorPrimaries`:

| gamma / primaries               | transform                     |
| ------------------------------- | ----------------------------- |
| `s-log3` / `s-gamut3`           | generated S-Gamut3 LUT        |
| `s-log3-cine` / `s-gamut3-cine` | generated S-Gamut3.Cine LUT   |
| `s-cinetone` / `rec709`         | none — passed through         |
| GoPro, DJI, anything unknown    | none — passed through         |

The two 33³ `.cube` files are computed in Go at first use and cached under
`<drive>/proxies/luts/`. Nothing binary is committed and nothing is downloaded.
A clip with no sidecar inherits the most common transform on its own card,
since capture settings do not change mid-card. A card with no sidecar anywhere
on it — a GoPro or drone card sitting in a mission alongside Sony ones — has
nothing to inherit and passes through.

Proxies mirror the mission layout on the target drive:

```
<drive>/proxies/<year>/<NNN>_<name>/
  edit/<card_volume>/<clip>.mov
  browse/<card_volume>/<clip>.mp4
  stills/<card_volume>/<clip>.{poster.jpg,sprite.jpg}
  proxies.b3
  proxies.json
```

`proxies.b3` is the same format as `checksums.b3` — sorted
`blake3_hash  relative_path` — so the existing verify machinery reads it
unchanged. `proxies.json` records, per clip, the source path and its BLAKE3,
duration, resolution, frame rate, codec, the detected gamma and primaries, and
which transform was applied. A re-run skips any clip whose recorded source hash
still matches, and reuses the cached sidecar reading rather than re-parsing
every XML.

Proxy filenames keep the source stem and change only the extension, because
Resolve pairs a folder of proxies to a selection of clips by filename excluding
extension. Relinking a mission is one action: select its clips in the Media
Pool, right-click → Link Proxy Media, and point at
`.../proxies/<year>/<mission>/edit/<card>/`. This was verified against Resolve
Studio 20.0.1 — see `PROXIES.md`.

**Proxies are derived, and are never archived.** `-sync` and `-replicate` skip
the `proxies/` root entirely: cold space is the scarce resource and every proxy
can be regenerated from the original. The stills tier is the exception — it is
small enough to copy anywhere, and `-index` does exactly that.

`-index` builds a self-contained static site from the proxy manifests and the
`checksums.b3` files across every mounted drive: `index.html`, one `index.json`,
and a copy of the stills tree. Plain HTML, CSS and JS, no build step and no CDN,
so it opens from `file://` with the drives unmounted. Navigate by year and
mission, hover a clip to scrub its sprite sheet, filter by mission, card, codec,
capture gamma or duration, and click through for the full metadata, a copyable
absolute path per drive holding the mission, and the `qcp -pull` command to
retrieve it. Clicking a clip plays its browse proxy when the drive it lives
on happens to be mounted, and shows the path when it is not — or, under
`qcp -serve`, plays it through the server regardless of what the browser is
allowed to read.

`-serve` puts that same index behind a local HTTP server, and is the answer when
the proxies will not play from `file://`. A page opened from disk can only reach
a proxy by absolute `file://` URL, and whether that works is up to the browser:
an `http://` page is not permitted to load `file://` subresources at all, and on
macOS a browser needs its own Files and Folders permission for a removable
volume before it can read one. `-serve` sidesteps both — qcp reads the drive,
which it already has access to, and the page only ever talks to the server. It
also gets seeking, which `file://` does not reliably give: the player asks for
byte ranges and `-serve` answers them.

Only the browse proxies listed in `index.json` are served, by exact path; the
originals and the rest of the disk are not reachable through it. It binds to
`localhost` by default — pass `-addr :8080` to browse the archive from a phone
on the same network, which prints the LAN URL to open.

### Flagging

Under `qcp -serve` each clip carries a flag toggle, and the toolbar gains a
**Flagged only** filter. `qcp -resolve` then pushes those flags into the open
DaVinci Resolve project: a flagged clip gets a blue flag *and* a blue clip
colour in the Media Pool. Matching is on the absolute source path, which
Resolve reports as `File Path` exactly as qcp composes it, so nothing is
guessed from filenames and a clip that exists on two drives cannot be confused.

It is one-way by design. The index is the source of truth, so there is no
conflict to resolve and nothing is read back out of Resolve. `-unflag` retracts
only the colour qcp itself applies, leaving a flag or clip colour set by hand in
Resolve alone. Flagged clips that were never imported into the open project are
reported and skipped — the index covers the whole archive, a project covers one
shoot.

Flags live in a `.qcp-flags.json` **dotfile in the mission directory**, beside
the footage they describe. `findFiles` skips any path component starting with a
dot, so the file is invisible to `checksums.b3`, `-verify`, `-check`, `-sync`,
`-replicate` and `-pull`. That invisibility is deliberate: a flag must never be
able to make a good mission look corrupt or an archive drive look out of date.
The cost is that flags are not carried to cold storage by a transfer. `-evict`
is the exception, because it removes the copy they live on: before deleting a
hot mission it merges its flags into the cold copies that justified the
deletion, newest timestamp winning, and refuses to delete anything it could not
carry them off. The flag store reads every mounted drive holding a mission, so
`-serve` and `-resolve` pick them up from a cold drive exactly as from a hot
one. Two further consequences worth knowing: only clips that have been proxied
can be flagged, because only those appear in the index; and flagging needs
somewhere to write, so it is offered under `-serve` and not when the index is
opened from `file://`, and only for a mission that is on a mounted hot drive.

Resolve's scripting API is a Python module inside the application bundle, so
`-resolve` shells out to `python3`. It needs Resolve running with a project
open, and **Preferences → System → General → External scripting using** set to
**Local**.

Browse-tier proxies are also generated automatically at the end of `-ingest`,
while the cards are still mounted, so backfilling the existing archive is a
one-time event rather than a permanent chore. Pass `-proxy=false` to skip it.
The edit tier is never generated at ingest.

### Organise

```sh
qcp -organise                         # group loose files into seasonal mission folders
qcp -organise -year 2025
qcp -reorganise                       # regroup already-organised missions by season
qcp -renumber                         # fix mission numbers to be sequential
qcp -init                             # sync sequence counter to what's on disk
qcp -init -year all                   # scan all years
```

`-organise` extracts shoot dates from filenames and media metadata (via `ffprobe`) and groups files into `NNN_Spring`, `NNN_Summer`, `NNN_Autumn`, `NNN_Winter` folders. `-reorganise` re-runs the same grouping over already-numbered missions. `-renumber` fixes duplicate or gapped numbers after any reorganisation.

`-init` is a recovery command: it scans the drives and resets the sequence counter to the highest mission number it can see. Raising the counter is always safe. Moving it *back* needs `-year` given explicitly, because what a scan can see depends on what is plugged in — with the archive unmounted and old missions already evicted off the hot drives, a bare `-init` would otherwise hand the next `-ingest` a number that already names a mission. When the counter is ahead of the drives, `-init` says so and names the flag that would apply the change.

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
-to <drives>     destinations for -copy / -proxy, or the index dir for -index / -serve
-addr <addr>     address for -serve to listen on (default: localhost:8080)
-unflag          -resolve: clear qcp's flag from clips no longer flagged
-tier <tier>     proxy tier: browse (default), edit, or both
-y               skip confirmation prompts
-version         print version and exit
```

---

## Typical workflow

```sh
# check drive space and what's already been ingested
qcp -status

# ingest from mounted cards — browse proxies are generated on the way out
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

# backfill proxies for an older year, then rebuild the browsable index
qcp -proxy all -year 2025
qcp -index
```

## Known inefficiencies

**Copying to N destinations reads the source N times.** Every copy job opens the
source file independently, so copying a 22GB mission to two hot drives reads
44GB. This affects `-ingest`, `-sync`, `-replicate`, `-pull` and `-copy` alike.

It only bites when the source cannot supply N× the destinations' write
bandwidth — two SSDs writing at 1GB/s each need 2GB/s of reads, so a 1GB/s
source becomes the ceiling. With one destination, or a source much faster than
the destinations, it costs nothing.

Fixing it means reading each file once in `job()` (`copy.go`) and fanning the
bytes out to N writers, hashing once on the way through. The awkward part is
error handling: a single read currently fails or succeeds for one destination,
and a shared read has to record a per-destination result so one failing drive
does not abort the others.
