# Bugs

Findings from two reads of the whole tree, both on 2026-08-25. `go build`,
`go vet` and `go test -race ./...` were clean before each of them, so nothing
here was caught by the existing suite. Line numbers for the first read are
against commit `d7d6706` plus the progress-bar fix; for the second, against
commit `156d41c`.

No open findings remain. The fixed entries are kept for context — each says what
was wrong, how it was confirmed, and what the fix does, newest first. Every
entry from the second read has a regression test that fails with the fix
reverted and passes with it in place; each was confirmed the same way before
being written.

The newest entry did not come from either read. It was found on 2026-09-01 by
running `-index` against a `-proxy` run that was still going; its line numbers
are against commit `111977c`.

A third read on 2026-10-06 looked only at the paths that copy, delete, rename or
record the hashes of footage. It started from a mission counter that had run two
ahead of the drives. Its line numbers are against commit `111977c` plus the
uncommitted proxy work that was in the tree at the time. All ten of its
findings are fixed below, along with the counter problem it started from.

A fourth read the same day covered everything the third did not: verify, check,
the transfer planning, drive detection, the ingest prompts, eject, serve and
flags, and the code added by the third read. All of its findings
are fixed below. One, that `-ingest` copies to cold drives as well as hot, was
confirmed as intended and is documented instead.

---

## Fixed

### `-serve` accepted flag changes from other origins

`/api/flag` (`serve.go`) decoded any POST body as JSON whatever its content type,
and never looked at where the request came from. A browser sends a cross-site
POST with a `text/plain` body without asking the server first, so any web page
open in the same browser while `-serve` ran could flag or unflag clips, given
the year, mission and clip path. `-resolve` pushes flags into the open Resolve
project. Paths were never at risk, since only clips the index published can be
flagged, but the flags themselves were.

Fixed with `sameOriginJSON`, checked before the body is read. The request must
be `application/json`. A browser cannot send that cross-site without a CORS
preflight, which qcp never approves. An `Origin` that is not the server's own
host (including `null`), or `Sec-Fetch-Site: cross-site`, is refused with 403.
The index page already sends `application/json` from its own origin, so it is
unaffected. A script on the same machine that sends neither header still
works.

Regression test in `serve_test.go`: the page's own request and a header-less
JSON client are accepted. `text/plain`, a foreign origin, a `null` origin, a
cross-site fetch and a missing content type are refused.

Left alone: `-addr :8080` still serves the whole LAN without authentication.
That is its documented purpose (browsing from a phone), and what it exposes is
proxies and flags, never footage.

### The catalog was not used by `-init` or by "mission not found"

Two places still went by the mounted drives alone after the catalog went in.

`-init -year 2026` moves the counter back to the highest mission on the mounted
drives, warning that every drive holding the year must be mounted. It did not
consult the catalog, so with the archive away it rewound below numbers the
catalog knew were spent. That is the same reuse hazard the explicit-year rule
exists to prevent, and the one case where the catalog had the answer.
Confirmed with the counter at 050, `030_Recent` mounted and the archive last
seen holding `042_Archived`: `-init -year 2026` set the counter to 030, and the
next ingest would have minted 031 through 042 again.

`findMissionSlug`'s "no mission 042_ found on any mounted drive" was all a
command like `-pull 42`, `-verify 42` or `-ingest 42` could say, even when the
catalog knew exactly which drive in the drawer held it.

Fixed:

- `runInit` (`init.go`) counts every unmounted drive as last seen. It raises the
  counter to cover those numbers, and never moves it back below them, even
  with `-year`. It prints which drive it is counting and when that drive was
  seen.
- `findMissionSlug` (`util.go`) now says "042_Archived is on ARCHIVE_01, which
  is not mounted (last seen 3 Sep)" when the catalog knows. A mission nowhere
  in the catalog still gets the plain message.

Regression tests in `catalog_test.go`: with the archive away, `-init -year 2026`
from 050 stops at 042, a bare `-init` from 7 rises to 042, and
`findMissionSlug(42)` names the archive. All three fail with the catalog
lookups disabled.

### `-pull` and `-copy` carried the source's `checksums.b3` as footage

`resolveSource` (`pull.go`) listed the source mission with `findFiles`, which
includes the mission's own `checksums.b3`. A whole-mission `-pull` or `-copy`
therefore copied the manifest across like a clip. The destination's manifest
became a byte copy of the source's, with every entry it held, including
entries for files that were not on the source to copy, and the run then merged
its own verified hashes on top. So the destination recorded files it never
received, which its next `-verify` or `-check` would report as missing from
disk. `-sync` and `-replicate` never did this, because they list content only.

Fixed by listing the source with `contentFiles`, as the others do. The
destination's manifest is now built from the hashes this run verified, plus
whatever the destination already recorded. It also stops the manifest's size
counting toward the plan and toward choosing the fullest source.

Regression test in `transfer_test.go`: a source whose manifest also lists a
`gone.mp4` that is not on disk. The manifest must not be among the files to
copy, and after `-copy` the destination must record `a.mp4` and not
`gone.mp4`. It fails with `findFiles` restored.

### `-check` never compared a second hot copy

`-check` picks a reference copy of each mission (the first hot drive holding
it, otherwise a cold one) and compared it with the cold drives scoped for the
year, and with nothing else. A second hot copy, T7 beside T9, was never
compared with anything. A file missing from it, or a different file under the
same name, went unreported until that copy was the one something read from:
the fullest-source rule in `-pull`/`-copy` and `-sync`'s cross-check catch some
of that, but neither is a report.

Fixed with `checkTargets` (`check.go`), used by the single-mission and year
paths. Every other hot drive that holds the mission is compared exactly as a
cold drive is: missing and extra files, sizes, and manifest conflicts. A hot
drive without the mission is not a gap, since hot drives are not expected to
hold everything. One holding part of it is. The summary lines now say "other
copies" rather than "cold drives", and point at `-copy` as well as `-sync`.

Regression test in `transfer_test.go`: T9 and the archive hold `a.mp4` and
`b.mp4` in mission 1, T7 only `a.mp4`. Both `-check` paths must fail and name T7
and `b.mp4`. A second mission held by T9 and the archive but not T7 must not be
reported, and `-check 2` must pass. With hot drives left out of
`checkTargets`, both paths pass mission 1.

### The config was never validated

`loadConfig` (`config.go`) parsed `~/.qcp` and checked nothing else, so:

- A role typo such as `"Cold"` left a drive out of every hot and cold path. It
  was never synced to, replicated, checked or counted by `-evict`, yet it still
  took a copy of every ingest, which goes to every mounted drive.
- Two drives with the same name collided in every map keyed by drive name (the
  per-drive bars, pools and drive probes) and shared one catalog file.
- Two entries resolving to one footage folder made one copy look like two.
  `-evict` now refuses that itself (see the entry below), but nothing reported
  it in the first place.
- A `year_from` after `year_to` silently excluded the drive from every year.
- A card with an empty `sub` matched the root of any external volume, so a stray
  USB stick would have been offered for ingest whole.

Fixed with `validateConfig`, which runs on every load and lists every problem
at once before qcp stops. It checks:

- every drive has a volume or a path and a role of `hot` or `cold`;
- names are unique (compared the way `-to`/`-from` compare them) and no two
  drives share a footage folder;
- `year_from`/`year_to` are years from 2000 to 2099, in order;
- `root` stays inside the drive;
- every card has a `sub` folder inside the card.

The config on this machine passes.

Regression tests in `config_test.go`: one config with each of those problems
must report every one, a valid three-drive config must pass, and the installed
`~/.qcp`, when there is one, must pass.

### The ingest prompt looped forever when input ran out

`promptMissionForDay` (`ingest.go`) read each answer with
`line, _ := reader.ReadString('\n')`. With no suggestion to fall back on, an
empty answer meant ask again, and a closed or exhausted stdin returns an empty
line and an error that was thrown away. So input piped from a script, or a
terminal that went away, left the prompt reprinting itself as fast as it could,
forever.

The prompts also each read stdin their own way. `confirm` used `fmt.Scan`, which
takes a word and leaves the rest of the line, newline included, to be read as
an empty answer by the next prompt. Every other prompt made its own
`bufio.Reader`, which can buffer input that was meant for the prompt after it.

Fixed with one shared reader and `readLine` (`util.go`), which every prompt now
goes through: `confirm`, `ask`, the mission prompt, and the delete-on-interrupt
questions in ingest, `-sync`, `-replicate` and `-pull`/`-copy` (now
`askYesNo`). At the end of input the mission prompt returns an error and the
ingest stops ("input ended"). A yes/no question reads end of input as no, so
nothing is deleted because input ran out. A last line without a newline still
counts as an answer. The multi-day ingest's error message for a failed prompt
said "err reading mission counter", and now says what failed.

Regression test in `ingest_safety_test.go`. The prompt is given empty input,
blank lines only, an unterminated answer and a skip, and must return within two
seconds each time. `ask` and `askYesNo` must answer no at the end of input. With
the read error ignored again, the prompt never returns on empty input.

### File sizes were never compared

`-sync`, `-replicate`, `-pull` and `-copy` decided which files a destination
already had by name alone. `-check` likewise compared only which names each
drive held. A truncated file, a different file under the same name, or a short
copy left by a tool other than qcp was taken for done: never copied, never
reported, and `-check` called the mission complete. Confirmed on `-sync`,
`-copy` and `-check` with a 5-byte cold `a.mp4` beside a 14-byte hot one: all
three passed.

Fixed with `planCopy` (`util.go`), which every transfer now uses. It splits the
source's files into those the destination lacks and those it holds at a
different size. A size mismatch is reported as a conflict and never
overwritten, because the destination may be the copy that is right. The rest
of the run goes ahead, and the command then fails, pointing at `-check` and
`-verify`. `-check` lists size differences beside its hash conflicts (`≠`, with
both sizes) in single-mission and year mode, and fails on them.

Regression tests in `transfer_test.go`: `-sync` copies the missing file, leaves
the wrong-size one untouched and fails; `-copy` fails the same way; `-check`
fails in both modes. All three fail with the size comparison in `planCopy`
switched off. A unit test covers `planCopy` itself.

### `-verify` named the mission instead of the drive, and passed what it had not checked

`runVerify` printed its FAIL line with `filepath.Base(dir)`, which is the mission
directory, where the drive name belonged (`verify.go:122`). With a mission on
two drives, a failure did not say which copy was bad, and that is the one thing
the user needs next. Its read-error line did not name the drive either.

It also passed things it had not checked:

- A copy with no `checksums.b3` got a warning and was skipped, and the mission
  passed on the other copies alone.
- Files on disk that the manifest did not record were never looked at, and the
  run still said "all N files ok".
- In year mode (`-verify all`), `verifySlug` printed a dim "—" for a mission with
  no manifest anywhere and returned success, while `-verify 42` on the same
  mission failed.

Fixed with `planVerifyCopy` (`verify.go`), which both paths now share. For each
copy it returns the recorded entries, the files on disk the manifest leaves
out, and a reason when the copy cannot be verified at all (no manifest, an
unreadable manifest, an unscannable directory). Failure lines carry the drive
name. A copy that cannot be verified, or a file that is not recorded, now fails
the mission with what and where, and points at `-checksum` where that is the
remedy. The recorded files are still verified, so one bad copy does not hide
the state of the others.

Regression tests in `verify_test.go`: the drive is named on a mismatch, and an
unrecorded file, a copy without a manifest, and a mission with no manifest
under `-verify all` each fail. All four fail against the old `runVerify` and
`verifySlug`. A fifth test checks that a good mission still passes both ways.

### Read errors in `checksums.b3` looked like a shorter manifest

Every reader of the manifest (`readChecksumFile`, and the two copies of the
parsing loop in `verify.go`) ran a `bufio.Scanner` to the end and never checked
`scanner.Err()`. A manifest that failed to open was also treated as absent. So
an I/O error part-way through a manifest, which is what a failing drive
produces, read exactly like a shorter manifest. `-verify` checked fewer files
and passed. `-evict` qualified a cold copy against fewer entries. Worst, the
merge after every transfer read what it could and wrote that back, dropping
every entry past the failure for good.

Fixed with `readChecksums` (`util.go`). A manifest that does not exist is
empty, but one that exists and cannot be read in full is an error. Every caller
that decides something or writes a manifest back now stops on that error:

- `mergeChecksums`/`addChecksums` refuse to write.
- `-verify` reports the copy as unverifiable.
- `-checksum`'s recorded-hash check and `missionFiles` fail.
- `-evict` refuses that copy.
- A transfer fails every file copied from a source whose manifest cannot be read.
- The ingest's already-copied check counts it as a collision.
- `-organise` neither carries nor rewrites that manifest.
- The catalog keeps its previous entry.

`readChecksumFile` remains for display and for derived data such as `-list`,
`-status` and the proxy planner, and now prints a warning instead of staying
silent.

Regression test in `verify_test.go`, using a directory in place of
`checksums.b3`, which opens but fails its first read. `readChecksums` and
`mergeChecksums` must return an error, `planVerifyCopy` must report the copy as
unverifiable, and a transfer from such a source must be refused. A missing
manifest must still read as empty without error.

### Mission names were barely sanitised

`sanitizeMission` (`util.go`) replaced spaces and nothing else, so a mission name
went into a path as typed. A `/` made nested directories: `Alps/Day 1` became
mission `045_Alps` with the footage one level down. `../` put the footage
outside the year directory entirely. The characters exFAT will not store
(`\ : * ? " < > |`) made the copy fail part-way on an exFAT drive.

Fixed: path separators, control characters and those exFAT-illegal characters
become `_`. Trailing dots and spaces are dropped, since exFAT and Windows drop
them silently and the name on the drive would no longer match the one qcp
recorded. A name with nothing left but separators and dots is refused: the
ingest prompt asks again, and `-ingest "<name>"` exits with an error.
Non-ASCII letters pass through unchanged.

Regression test in `ingest_safety_test.go`. Every case must come out as a single
path component free of those characters. Under the old function the `/` and
`../` cases produced nested paths and failed.

### Every run left a `caffeinate` running for good

`keepAwake` (`util.go`) started `caffeinate -mi` to keep the Mac and the drives
awake during long copies. Its comment said the process "is killed automatically
when the process exits", but nothing killed it, and on macOS a child is not
killed with its parent. Every qcp run therefore left a `caffeinate` behind,
reparented to launchd, holding the Mac out of idle sleep and its disks out of
spin-down for good. Found on 2026-10-06 with 382 of them running, the oldest
for 47 days. `pmset -g assertions` showed sleep and disk idle blocked for 1,126
hours.

Fixed by passing `-w <qcp's pid>`, so `caffeinate` exits when qcp does, however
qcp ends: a normal return, `os.Exit`, a panic or `kill -9`. The process is
also reaped if it ends first. The stray ones were cleared with
`pkill -x caffeinate`.

Regression test in `keepawake_test.go`: a child process starts `keepAwake` and
exits, and its `caffeinate` must be gone within five seconds. It fails with
`-w` pointed at a process that never exits, which is equivalent to leaving it
out.

Not a bug, but in the same read: `-ingest` copies to every mounted drive, cold
ones included and regardless of `year_from`/`year_to`, where the README said hot
drives only. That behaviour is intended, since a cold drive plugged in at
ingest gets a copy verified straight from the card, so the README and `-help`
now say so instead.

### A failed or abandoned ingest left the mission counter ahead of the drives

Found 2026-10-06 from the outside: `~/.qcp_seq` held 046 for 2026 while the
highest mission on T9 was 044. The counter was given back in only one case:
Ctrl-C followed by agreeing to delete the partial mission. Everything else left
a number spent on a mission that never existed:

- A copy or verify failure exited (`exit(10)`, `exit(11)`) with the number
  committed and no way to give it back.
- A multi-day ingest committed every day's number before the first day
  started. Abandoning day one left the numbers for the later days spent.
- Giving a number back decremented the counter whatever it held, so it could
  not tell the number that failed from a later one, or a kept partial copy
  from one that was deleted.
- The number was committed before the interrupt state recorded it, so a Ctrl-C
  in between kept it.

One more thing in the same handler could destroy footage. Ctrl-C during an
*append* offered to "delete partial mission" and, on yes, ran `os.RemoveAll` on
the existing mission's directories, including everything ingested into them
before.

Fixed:

- Numbers are committed one day at a time, as each day starts.
- Copy and verify failures go through the same cleanup as Ctrl-C (`abandon` in
  `main.go`). Only a new mission is offered for deletion. An append keeps what
  it copied for a re-run to finish.
- `releaseMission` (`seq.go`) replaces the decrement. It gives back exactly the
  number that failed, only while that number is still the last one handed out,
  and only if no mounted drive holds a mission with that number. A kept partial
  copy keeps its number, and a failure before anything landed gives it back
  without asking.
- A new mission refuses to copy into a directory that already exists
  (`refuseExistingRoots`). That can only happen when the counter is behind the
  drives and the name matches too.

Every ingest now also runs `checkMissionCounter` (`seq.go`) before it hands out
a number:

- A counter behind the drives would reuse a number, so qcp warns and offers to
  raise it. Under `-y` it raises it without asking.
- A counter ahead of the drives is reported only when every drive that can hold
  the year is mounted, since otherwise the missing numbers may be on the
  archive. Moving it back needs a typed yes, so `-y` only warns.

Regression tests in `seq_test.go`: `releaseMission` gives back an unused number,
leaves a later one alone, and keeps the number of a kept partial copy.
`checkMissionCounter` raises a counter behind the drives, does not rewind under
`-y`, stays quiet with a drive away, and leaves a counter in step untouched.

### `-evict` did not check that a hot and a cold copy were different directories

`qualifyBackups` (`evict.go:209`) accepted any cold drive entry whose mission
directory held every hot file in an agreeing manifest. Two configured drives can
resolve to one folder: a `path` entry and a `volume` entry for the same disk, a
symlink, or a copy-pasted config line. If they did, the "cold copy" was the hot
copy itself. It had every file, its manifest agreed with itself, and it passed
verification, so `-evict` deleted the only copy it had just declared safe. In
the same way, two entries for one cold folder counted as two copies toward
`-copies`. Confirmed with the cold drive's path a symlink to the hot drive: the
hot copy qualified as its own backup. With a second symlinked cold entry, one
folder satisfied `-copies 2`.

Fixed with `sameDirAs` (`evict.go`). Before a cold copy is considered, it is
compared by filesystem identity (`os.SameFile`, not by path, since the paths
are what differ) with every hot copy about to be deleted and every cold copy
already counted. A match is refused with a note naming the drive it duplicates.

Regression tests in `evict_test.go` for both shapes. Both fail with the check
disabled.

### `-organise` could file the same clip into different missions on different drives

`runOrganise` dated each drive's loose files independently. The last fallback,
after ffprobe and the filename, is the file's mtime (`organise.go:462`). That
belongs to the copy, not the footage. `job` did not carry the source's mtime
across, so a cold copy was dated by when it was synced. The hot and cold copies
of one clip could then be filed into different season missions, and the drives
disagreed about which mission held the clip from then on. Confirmed with
`card/clip.mp4` on two drives, mtime July on one and November on the other:
one drive filed it under `001_Summer`, the other under `002_Autumn`.

Fixed in two places:

- `agreeOnDates` (`organise.go`) runs after every drive is scanned and gives
  every copy of a path the same date. The best-sourced date wins (ffprobe, then
  the filename, then the mtime), and among mtimes the earliest, which is
  nearest the recording.
- `job` (`copy.go`) now stamps every copy with its source's mtime, so new copies
  carry the recording time instead of the time they were made. The proxy cache
  checks the source hash first and size plus mtime only as a fallback, so
  nothing relied on a copy's mtime being the time it was copied.

Regression tests: `organise_test.go` checks that the July/November pair lands in
`001_Summer` on both drives, and `copy_test.go` checks that a copy keeps its
source's mtime. Both fail with the fix reverted.

Left alone: copies made before this keep the mtime of when they were made. Only
a drive's oldest copy of a file is likely to carry the real date, which is why
the earliest mtime wins.

### `-clean` walked the whole of a drive whose `root` is empty

With `-year all`, `runClean` walked the drive's footage root (`clean.go:28`).
A bare `-clean` is scoped to the current year and was not affected. T9 is
configured with `"root": ""`, so on T9 that was the whole volume:
`.Spotlight-V100`, `.Trashes`, `.fseventsd`, the proxy tree and anything else
kept on the drive. `-clean` deleted `._*` files from all of it, along with every
empty directory two or more levels down. On a non-footage file a `._` file
holds its Finder metadata and extended attributes. Confirmed with a
`Personal/._notes.txt`, an empty `Personal/a/b` and an empty directory inside
`.Spotlight-V100`: all three were removed.

Fixed with `cleanRoots` (`clean.go`). `-clean` now walks only year directories,
named 2000–2099 by the same rule `allYears` uses: the one asked for, or with
`-year all` every one under the footage root. Since every scan root is now a year, the
empty-directory depth is a constant: a directory inside a mission may go, a
mission or the year may not, as before.

Regression test in `clean_test.go`: junk and an empty directory inside a mission
are removed, and the three outside the footage survive. Fails with the fix
reverted.

### `-organise` deleted `checksums.b3`

`executeOrganisePlan` (`organise.go:439`) deleted the manifest of every
directory a file was moved into or out of. A rename does not change a file's
content, so the hashes recorded when the footage was known good were thrown away
for nothing. That included the files that stayed behind in a directory one file
had left. The next `-checksum` then recorded whatever was on disk. Confirmed
with a `-reorganise` that splits one recorded mission into a Summer and a Winter
mission: both new missions came out with no manifest at all.

Fixed with `manifestMoves` (`organise.go`). Each successful move takes its entry
out of the manifest of the top-level directory it came from and records it under
the file's new name in the destination. Every manifest that changed is rewritten
through `writeChecksums`. One left empty is removed so that `removeEmptyDirs`
can still collapse the directory it was in. A file the source manifest did not
mention is moved without an entry, as before.

While there, moves go through `moveNoReplace`. `os.Rename` silently replaces an
existing file, and the plan's collision handling only considers the files it is
moving, not what is already on disk. Every destination is a freshly numbered
mission or `_unsorted`, so I could not construct a case where that bites today,
but a rename that can destroy a file on a slip should not be left that way.

Regression tests in `organise_test.go`: the Summer/Winter split must carry both
hashes and remove the emptied mission (fails with the fix reverted), and a move
onto an existing file must be refused.

### `-checksum` overwrote recorded hashes without comparing them

Both `-checksum` paths (`checksum.go:261` for a year, `checksum.go:431` for one
mission) hash every file of any copy that is not fully checksummed. They then
replace its `checksums.b3` with the result and never look at what the manifest
already said. A mission only has to gain one unrecorded file, such as an
append, to be rehashed. So a file that had rotted since ingest had its good
hash overwritten by the bad one, the run printed ✓, and the evidence `-verify`
and `-evict` depend on was gone. Confirmed with a manifest recording `a.mp4` as
`original`, the file changed to `rotted`, and an unrecorded `b.mp4` beside it:
the manifest came back with the rotted hash and `runChecksum` returned true.

Two more ways it lost the record:

- Copies that were already fully checksummed were left out of the run and
  never consulted. A cold copy hashed on its own recorded whatever it held, even
  when the hot copy's manifest said otherwise.
- The rewrite listed only the files on disk now. Any entry for a file that had
  gone missing was dropped, and with it the only sign the file was gone.
  `missionFiles` counted these but both callers ignored the count.

Fixed with `recordedConflicts` and `recordedHashes` (`checksum.go`). Every fresh
hash is compared with the hash for that file in the drive's own manifest and in
the manifest of every other mounted copy of the mission. Any disagreement is a
conflict: the manifest is not written, the run fails, and the message points at
`-verify`. A copy whose manifest lists missing files is not rewritten either.
Hashes that agree are written back unchanged, so an ordinary append records
the new files and keeps the old entries.

Regression tests in `checksum_test.go`: the rotted file through both paths, a
cold copy that disagrees with a fully checksummed hot copy, and a recorded file
gone missing. Each must fail and leave the record alone, and all fail with the
fix reverted. A fourth test checks that a plain append still succeeds.

### `-renumber` renamed only the drives that were mounted, and deleted their manifests

`runRenumber` skipped any drive that was not mounted (`renumber.go:25`). A
mission number names one mission across every drive, so renumbering with the
archive in a drawer left it under the old numbers. That gave the same mission
two numbers, and a number on the hot drive named a different mission than the
same number on the archive. Missions that were only on the unmounted drive
could not be seen, so they could be handed numbers that were already taken.
The counter was then set to the number of missions it could see
(`renumber.go:130`), which moved it back below numbers already spent. That is
the same hazard as the bare `-init` entry below, by a different route.
Confirmed with missions `003_C` and `007_G` on a hot drive, the counter at 7
and the archive unmounted: both were renamed and the counter dropped to 2.

It also deleted each renamed mission's `checksums.b3` as stale
(`renumber.go:117`). The manifest's paths are relative to the mission
directory, so renaming the directory changes none of them. Deleting it threw
away the hashes recorded when the footage was known good, and the next
`-checksum` re-recorded whatever was on disk by then.

Fixed by refusing to run unless every drive that can hold the year (by
`year_from`/`year_to`) is mounted, naming the ones that are not, and by leaving
`checksums.b3` in place.

Regression tests in `renumber_test.go`: with the archive away nothing is renamed
and the counter stays at 7; with everything mounted the manifest survives the
rename. Both fail with the fix reverted.

Left alone: the proxy tree (`proxies/<year>/<slug>`) is not renamed with the
mission, so a renumbered mission's proxies are orphaned and regenerated on the
next `-proxy`. Proxies are derived, so this costs time and disk space, not
footage.

### `-ingest` skipped a card file whose name was already in the mission

`main.go:704` queued a card file for copying only if nothing existed at its
destination, and judged that by the name alone. Cards land under
`<mission>/<card volume name>/`. Card volume names repeat ("Untitled", "NO
NAME"), and camera clip counters can reset. So a second card appended into a
mission could match a clip from the first card by name, and that clip was
reported as already up to date and never copied. The card is formatted once
the ingest says it is done, which made that clip unrecoverable.

Fixed by `checkAlreadyCopied` (`ingest.go`). Every card file whose destination
already exists is checked before anything is copied. Its size must match, and
the card file must hash to what the drive holds. That is the mission's
`checksums.b3` entry when there is one, or the file on the drive when there is
not. Anything that does not match stops the day with nothing copied and a list
of the collisions; if the mission was new, its number is given back first. A
file that matches but was missing from the manifest is added to it. That state
is left by a run killed between copying and writing the manifest, and those
files used to stay unrecorded for good. The cost is one read of each such file
from the card, and only on a re-run or an append, which is where the danger is.

Regression test in `ingest_safety_test.go`, covering five files with taken
names: recorded and matching, matching but unrecorded, a different size, the
same size with different content, and a manifest that disagrees with the card.
Only the first two may pass, and only the second is recorded. The wiring into
the ingest flow is not under test, for the same reason as the entry below.

### A file that failed verification stayed on the drive under its final name

`-ingest` (`main.go:832`), `-sync` (`sync.go:398`), `-replicate`
(`replicate.go:385`) and `-pull`/`-copy` (`pull.go:344`) reported a
verification failure and then exited, leaving the bad copy at its destination
name. Every one of those commands decides what still needs copying by whether
the destination name exists. So a re-run skipped the bad file as already
copied, it was never verified again, and the next `-checksum` recorded its hash
as the good one. On `-ingest` the card is formatted once the run looks
finished, so the bad copy became the only copy.

The copy phase had the same hole one step earlier. When any file failed to
copy, all four exited before the verify phase. Every file that *had* copied was
then on disk under its final name, never read back and in no manifest, and
re-runs skipped those files too.

Fixed in all four places. A file that fails verification is removed at once by
`discardUnverified` (`copy.go`). Only files this run wrote ever reach it, and
the source still has the good copy. A copy failure no longer skips the verify
phase. The hashes of everything that did verify are written to `checksums.b3`
before the command reports what failed and exits non-zero.

Regression tests in `transfer_test.go`. They run the command in a child process
(`subprocess_test.go`), because these failures end in `os.Exit`. The first
gives `-sync` an unreadable source file and asserts that the file which did
copy is verified and recorded. The second test, under the next entry, asserts
that a rejected copy is removed. Both fail with the fix reverted. The ingest
change is the same few lines but has no test, because the ingest flow lives
inside `main()` and needs mounted cards.

### Drive-to-drive copies never checked the source against its own manifest

`-sync`, `-replicate`, `-pull` and `-copy` verified the destination against the
hash taken while reading the source, which proves only that the copy is
faithful. A source file that had rotted since it was recorded was copied
faithfully too, and the destination's manifest then recorded the damage as the
good hash. For hot-to-cold copies `-evict`'s manifest cross-check would later
catch the disagreement. Nothing caught it for cold-to-cold (`-replicate`) or
into a hot drive (`-pull`, `-copy`), so the damage spread with a clean
manifest. Confirmed on `-sync`, `-copy` and `-replicate`: a source whose
`checksums.b3` records different content was copied, recorded and reported as
success.

Fixed by comparing the bytes as read with the source's manifest. `sourceSums`
reads each job's source `checksums.b3` once, and `sourceMismatch` (`copy.go`)
checks every copied file before it is verified. A mismatch fails the file,
removes the copy, and points at `-verify` on the source. A file the source
manifest does not mention has nothing to be checked against and goes through
as before. The hash was already computed during the copy, so the check costs
nothing.

Regression tests in `transfer_test.go`, one each for `-sync`, `-copy` and
`-replicate`: a rotted source file must not reach the destination or its
manifest, the file beside it must be recorded, and the run must exit non-zero.
All three fail with the fix reverted. `-pull` shares `runTransfer` with
`-copy`.

### `checksums.b3` was written in place

Six sites wrote the manifest with `os.WriteFile`: the ingest, `-sync`,
`-replicate` and `-pull` merges, both `-checksum` paths, and the junk pruning
in `-clean`. `os.WriteFile` truncates the file before writing it, so a crash or
a drive unplugged mid-write left the manifest cut short. The manifest is the
only record of what the footage hashed to when it was known good, so every hash
past the cut was lost. `readChecksumFile` also skips a line it cannot parse, so
the damage was silent. The next `-checksum` would then re-record the missing
files from whatever was on disk by then.

Confirmed with a reader racing fifty rewrites of a 2,000-entry manifest: with
the in-place write it saw a short manifest almost at once.

Fixed with `writeChecksums` and `addChecksums` (`util.go`). Every manifest
write now goes through `writeFileAtomic`, which writes a temporary and renames
it over the manifest. `writeFileAtomic` now also syncs the temporary before the
rename, so after a power cut the new name cannot point at contents that never
reached the disk. The mission counter (`seq.go`) is written the same way, since
it is the one record of which numbers are spent.

Regression test in `manifest_test.go`: the racing reader must always see the
full manifest. It fails with the in-place write restored.

### Hours of finished proxies stayed invisible to `-index` until the run ended

Found from the outside: `qcp -year 2025 -proxy 1,2,...,42` had been running two
and a half hours on T9 with ~600 renditions on disk, and `qcp -index` reported
almost all of those missions as unproxied. Only the two missions whose manifests
came from *earlier, completed* runs had any clips in the index.

`-index` never walks the proxy tree. Per mission it reads `proxies.json` and
nothing else (`index.go:218`), so a mission with no manifest indexes as having
no clips however many renditions, posters and sprites sit beside it. And
`generatePlans` wrote every manifest in one loop after `wp.wait()` for every
pool across every mission in the batch — the accumulated `metas`/`writes` maps
lived in memory for the whole run. So the manifests were correct, just written
hours after the work they describe, and a 243-clip mission would have been
invisible for hours even under a per-mission write.

The same deferral was a durability hole. The interrupt path was already handled
— the manifest loop ran before `os.Exit(130)`, so Ctrl-C recorded everything
finished — but a `kill -9`, a panic or a power cut lost the bookkeeping for the
entire run, and the next run re-encoded all of it.

Fixed by writing each mission's manifest as every clip lands. `planState`
(`proxy.go:1133`) holds one mission's entries by `rel`, seeded at construction
with the entries carried forward from a previous run, and `record`
(`proxy.go:1161`) adds the finished clip and rewrites both manifests. Because
every write is the mission's complete picture rather than a delta, a partial run
still never drops what an earlier one recorded — the property the old
end-of-run loop got from re-appending cached entries. `writeProxyManifests`
already merged into the `proxies.b3` on disk rather than replacing it, so it
took no change to be called repeatedly.

The cost is nil. Each rendition is hashed exactly once either way; the only
addition is rewriting two small files per clip, against a clip that took tens of
seconds to encode.

Both files now go down through a temporary and a rename (`writeFileAtomic`,
`util.go:287`). This is required, not tidiness: `-index` reads `proxies.json`
while a run is writing it, and `readProxyManifest` (`proxy.go:134`) treats a
parse error as an empty manifest — so a torn read would drop the whole mission
out of the index silently, which is the bug being fixed here reappearing as a
rare non-deterministic one. Confirmed: with the atomic write reverted, a reader
racing 50 writes of a 200-clip manifest sees `0 clip(s)`.

The end-of-run loop survives as a reconcile pass for missions that encoded
nothing because every clip was cached — those never reach a worker, so nothing
records a clip for them, and they still need a manifest if an earlier run never
wrote one.

Three regression tests in `proxy_test.go`: that the manifest and `proxies.b3`
name each clip as it is recorded rather than at the end, that a cached-only
mission still gets a manifest while a mission with nothing recorded does not,
and that a concurrent reader never sees a torn manifest. The first two fail with
`record` reduced to accumulating, the third with the atomic write reverted.

Left alone: the tree lock. Per-clip writes narrow the window between two
concurrent runs but do not close it, because the stale half of the picture is
each run's *plan*, fixed when it planned the mission, not its write. The
rationale comment in `lock.go` and the section in `PROXIES.md` are reworded to
say so — both previously rested on "written once at the end of a run". The entry
below on a failed re-bake also refers to that loop by its old line; the
invariant it describes is now carried by the `planState` seeding.

### Every progress-bar phase hung instead of reporting a read or copy failure

`barTracker.flush()` (`progress.go:47`) drains pending bytes into the bar; it
does not make the bar complete. mpb fires the complete event only when `current`
reaches `total` and releases `Progress.Wait` only on that event, so a bar left
short blocks `Wait` forever — the same mechanism as the `total <= 0` hang below,
in its partial-fill form, and not fixed by that fix.

Every failure path leaves a bar short, because the failed file's bytes went into
the total and never into the progress. And every one of these phases reports its
failures on the line *after* the `Wait`, so the effect was that the ERROR line
printed and the run then sat there behind a stalled bar with the summary,
the exit code and — on `-ingest` — the `exit(10)`/`exit(11)` all unreachable.
Twelve sites: `verify.go:133`, `checksum.go:268` and `394`, `main.go:727` and
`802`, `sync.go:342` and `416`, `replicate.go:323` and `396`, `pull.go:286` and
`361`, `evict.go:371`.

Confirmed against `runVerify`, `runChecksum` and `runCopy` on real temporary
drives with one `chmod 000` file standing in for a bad sector: each printed its
error and had not returned five seconds later. `-verify` is the sharpest case,
since an unreadable file is the thing it exists to find. A hash *mismatch* was
never affected — that still reads the whole file, so the bar fills and the
failure is reported correctly.

Worse on `-ingest`, where the hang happens with `intr` still armed: the Ctrl-C
needed to get out lands in the interrupt handler, which offers to delete the
mission that was in flight.

Fixed with `barTracker.stop()` (`progress.go:66`) — flush, then `Abort(false)`,
which mpb treats as a no-op on a bar that already completed. All twelve sites
call it instead of `flush()`. Abort rather than top-up because a byte-exact bar
that lands short means a file did not make it, and the bar should say so: it
stops where it got to rather than showing a total it never reached. `finish()`
is unchanged and still tops up, because the two callers it has — `index.go:307`
and `proxy.go:1223` — estimate progress from ffmpeg's reported time and land
legitimately short.

Regression tests in `progress_test.go` alongside the zero-total ones, asserting
both directions: a bar left short releases `Wait` and comes out aborted, a bar
that filled comes out complete rather than aborted, and one short bar does not
strand the other bars on the same container.

Left alone: `flush()` itself, which is still the right call mid-phase, and the
fact that a failed phase now shows a partly-filled bar rather than a bar that
disappears — which is the point.

### `-organise` took `000_*` missions apart

`scanUnorganised` (`organise.go:224`) decided "already in a mission, leave it
alone" with `isNumberedMission(top)`, which requires `n > 0`. It was the last
caller of that predicate that is not resolving a mission *number* — `-proxy` and
`-renumber` genuinely are — and so the one the `000_*` sweep below missed.

A plain `qcp -organise` therefore walked into `000_Edits`, dated its contents by
mtime like any loose file and planned to move them into `NNN_Season`;
`removeEmptyDirs` then took the emptied `000_Edits` away. Confirmed end to end
on a temporary drive: `000_Edits/cut_v3.mov` came out as `043_Summer/cut_v3.mov`
and the year directory held only `043_Summer`. README.md:92 is explicit that
these are missions and only unaddressable *by number*.

Fixed with `skipOrganise` (`organise.go:529`), which parses the number once and
answers for both commands: `-organise` groups what is not yet in a mission, so
every mission is off limits to it; `-reorganise` does re-bucket missions, which
is what it is for, but `000_*` sits outside the numbering by construction — a
named mission rather than a season's worth of footage — so it is left alone by
that too. That second half is a deliberate widening beyond the reported bug:
regrouping `000_Edits` into `NNN_Winter` is meaningless and there was no way to
opt out of it.

Regression test in `organise_test.go` on both `scanUnorganised` — asserting the
exact file list for each of `regroup` false and true, so it pins what
`-reorganise` still does pick up as well as what neither touches — and on
`skipOrganise` as a table.

### A bare `qcp -init` rewound the mission counter to whatever was mounted

`main.go:334` passed `!yearAll` as `runInit`'s `yearExplicit`, so a bare
`qcp -init` took the branch whose own comment said "when a year is explicitly
requested" — and that branch drops the `max > current` guard and lets the
counter move *down* (`init.go:62`).

The counter is a promise never to mint a number twice, and what a scan can see
depends on what is plugged in. The archive being in a drawer is the normal
state, and `-evict` exists to take old missions off the hot drives, so the
ordinary shape of this tool produces exactly the situation where the visible
maximum is far below the counter. Confirmed with `seq[2026] = 42` and only a
drive holding `030_Recent` mounted: `2026: 042 → 030`, after which the next
`-ingest` mints `031` over a mission that already exists on the unmounted drive.

Fixed by separating the two things the one flag controlled. `runInit` now takes
`scopeToYear` (which year directories to scan, still `!yearAll`) and `rewindOK`
(`*yearFlag != "" && !yearAll`), so only a `-year` the user actually typed
licenses a rewind. Raising is unconditional, as before. A declined rewind is not
reported as "already up to date" any more — it says what the drives show and
names the flag that would apply it — and an accepted one prints a warning that
every drive holding the year has to be mounted.

Regression test in `init_test.go`: the same unmounted-archive fixture, asserting
the counter holds at 42 without an explicit year, moves to 30 with one, and
still rises to 30 from 7 either way.

Left alone: `-init -year 2026` still rewinds on a partial view if that is what
you ask for. That is the documented repair for a counter that ran ahead, and the
warning is what makes the requirement explicit.

### `-replicate` could never fill a gap in the first cold drive

`runReplicate` took the first cold drive holding a mission as its source
(`replicate.go:108`) and diffed every other copy against it. A file missing *on
that drive* was therefore not missing at all — the drive listed first in the
config silently defined the mission — and the run printed `cold drives are in
sync` over an archive that was short.

Confirmed with `ARCHIVE_01` holding `{a.mxf}` and `ARCHIVE_02` holding
`{a.mxf, b.mxf}`, config order as written: no jobs, "cold drives are in sync",
`b.mxf` still absent. README.md:148 sells this exact case — "to catch up a drive
that wasn't present during `-sync`" — and it worked only when the stale drive
happened to sort second.

Fixed by keeping the fullest copy as the source, the same rule and the same
reason as `resolveSource` (`pull.go:404`) on the pull side: a partially-synced
drive must never be silently used as the source. The cost is a directory walk
per cold copy rather than per mission, which is what `-sync` already pays across
its primaries.

Regression test in `replicate_test.go`, running the command end to end on two
temporary drives and asserting both that the file lands and that it reaches the
receiving drive's `checksums.b3` like any other transfer's.

Left alone: two cold copies that disagree about a file's *contents* are still
not detected here — both hold it by name, so neither is missing anything.
`-check` and `-verify` are the tools for that, and `-sync`'s manifest
cross-check has no counterpart on this side.

### A failed proxy re-bake recorded itself as up to date

`generateClip` stamps the new `SrcHash` (`proxy.go:819`) and `Transform`
(`proxy.go:823`) onto the manifest entry before doing any work, and
`generatePlans` appended the returned entry whether the clip had succeeded or
not (`proxy.go:1211` before the fix). Those two fields are exactly what `planMission` tests to
decide a clip is stale, so writing the entry for a failed clip cleared the very
trigger that said the rendition on disk was out of date.

The next run then read a proxy carrying the *old* look as up to date, and every
run after it did too. `BrowseSpec` and `EditSpec` never had the problem, because
they are the two fields assigned only after a successful encode — which is what
identified the asymmetry as the bug rather than the design.

Confirmed with a clip recorded under `look/old@deadbeef`, its browse rendition,
poster and sprite on disk and its source unchanged: the first plan has
`todo = 1`, the encode fails, and the second plan has `todo = 0` with
`transform: "none"` recorded against the old rendition. The way in is ordinary —
configure or edit a look, run `-proxy all`, have one clip fail for any reason.

Fixed by recording nothing for a clip that failed. The loop that keeps entries
from previous runs (`proxy.go:1244`) already re-appends the last *successful*
entry for any cached clip that was not regenerated, so the old transform ID
survives and the clip stays stale. A clip that had no previous entry drops out
of `proxies.json` entirely, which is correct: nothing was generated for it.

Regression test in `proxy_test.go`, using a file ffprobe refuses as the stand-in
for any encode failure, asserting both the recorded transform and that the next
plan still has work to do.

Left alone: the conservative half of this. When the browse tier encodes and the
poster then fails, the whole entry is dropped, so the next run re-encodes the
browse rendition it already has. Redoing work is the right failure for an
archival tool, and the alternative — recording part of an entry — is the bug.

### `-check` failed over a file `-sync` will never copy, and told you to run `-sync`

`-check` listed a mission with `findFiles`, which includes `checksums.b3`, while
`-sync` and `-replicate` plan from `missionFiles`, which deliberately excludes
it. A cold copy that had not been checksummed yet therefore read as a copy
missing a file: `− checksums.b3`, counted into the total, exit 1, and
`run -sync to copy missing files to cold drives` — which never copies one. The
reverse pairing reported it as an extra file on the cold drive.

Confirmed on one tree: `-sync` printed `all drives are in sync` while `-check`
printed `1 files missing from cold drives` and returned false. A cold drive
unmounted during `-checksum` is the ordinary way in, and there is no `-sync` that
resolves it — only `-checksum` on the drive that is short.

Fixed with `contentFiles` (`util.go:135`), one answer to "what is this mission" that
the planner and the checker share: everything `findFiles` sees, less the
`metadataFiles` names at the top level. `missionFiles` and `isFullyChecksummed`
now build on it — which widens their existing `checksums.b3` skip to the other
three, none of which can legitimately be footage — and the four `findFiles`
calls in `check.go` go through it.

Regression test in `check_test.go`: the same mission with neither copy
checksummed, then each in turn, asserting `-check`, `-check 42` and `-sync` all
agree it is complete in all three cases. Plus a unit test that `contentFiles`
excludes only top-level bookkeeping — a nested `checksums.b3` is footage until
something says otherwise, matching what the transfers carry.

Left alone: `scanMissions` (`status.go:173`) still inlines its own
`checksums.b3` skip for the sizes `-list` prints. It agrees with the helper on
the only file that can occur there, and routing it through would mean giving up
the single walk it does.

### `-evict` destroyed the mission's flags

`qualifyBackups` proves every *file* survives on cold, and `runEvict` then
removed the whole hot directory (`evict.go:134`) — which took
`.qcp-flags.json` with it. Flags are deliberately never synced, so there was
nothing to bring them back from: the one piece of state in qcp a person creates
rather than derives, destroyed by the one command that deletes, with no warning
in the plan and no way back. Confirmed by evicting a mission with a flag set and
finding the file gone.

Fixed by carrying them. `targetFlags` merges the flags across the hot copies
about to go, `carryFlags` merges that into each qualifying cold copy — newest
timestamp winning, the same rule `mergeMissionFlags` already applies across
drives — and a mission whose flags could not be read or written is refused
rather than deleted, matching the stance `flagStore.read` takes and the one
`qualifyBackups` takes on an unreadable manifest. The plan says how many flags
are being carried.

Writing to a cold drive here breaks no invariant: a dotfile is invisible to
`findFiles`, `checksums.b3`, `-verify` and `-check`, so it cannot make an
archive look out of date, and the cold drive is already mounted and being read
at that point. `flagStore` reads every mounted drive holding a mission, so
`-serve` and `-resolve` pick a cold copy's flags up exactly like a hot one's.

Regression test in `evict_test.go`: a flag on the hot copy and an older,
different one already on the cold copy, asserting both survive the eviction and
that `flagStore.read` returns the union afterwards from the cold drive alone.

Left alone: flags on an evicted mission become read-only, because
`flagStore.set` writes to hot drives only and there is no longer a hot copy.
That is the pre-existing rule about never spinning up an archive HDD to toggle a
flag, and it now fails with a message rather than silently.

### `-ingest` had no interrupt gate after either phase

`runDay` was the fourth copy-then-verify function in the tree and the only one
with no `ctx.Err()` gate at all, after the other three were fixed to stop on
both phases. An interrupt during either phase therefore ran on to write
`checksums.b3`, print `✓ Done … copied and verified`, call `intr.clear()` and
start `runIngestProxies` — ffmpeg competing for the terminal while the handler
was still waiting on stdin for the delete prompt. If the answer was `y` the
mission was then removed, after the run had already claimed it verified.

Worse than the sync case, because the workers' own `ctx.Err()` guards made it
look clean: the copies and verifies that had not started returned early, so
`copyFailed` and `verifyFailed` were both zero and the manifest was written from
whatever subset happened to finish.

Fixed by adding the same gate after both `p1.Wait()` and `p2.Wait()`, with the
same comment as the three sites in `pull.go`, `sync.go` and `replicate.go` — all
four copy-then-verify functions now stop identically.

No regression test, for the same reason as the `sync.go`/`replicate.go` gates:
`runIngest` closes over `runDay`, which scans real cards, prints and installs a
signal handler that calls `os.Exit`, so the interrupt path is not reachable from
a unit test without splitting it up first.

Left alone: the wrong-mission-deleted failure was never reachable — `intr.get()`
is called at the top of the handler, so the snapshot is taken before
`intr.clear()` can run — but every other consequence was. Also left: the gate
stops the goroutine, it does not undo the copies in flight; those are already
safe by the `.qcp-part-` rename.

### `000_*` missions were never hashed, checked or verified

README.md says `000_*` directories "are synced like any mission but cannot be
addressed by mission-number commands", and `-sync` (`sync.go:56`) did carry
them: it takes every directory under the year that is not the proxy tree. But
every command that enumerated missions for itself filtered with
`isNumberedMission`, which requires `n > 0`, so `000_Edits` got no
`checksums.b3` from `-checksum` (both the year-wide walk and a targeted
`-checksum NNN`, which cannot name it), was not compared by `-check`, not
re-hashed by `-verify`, not indexed, and its flags were not collected. `-sync`
wrote the copies; nothing ever checked them. Noticed while fixing the listing
filter below.

Fixed by splitting the same way `-list` and `-status` were split: the commands
that operate on *whatever is on the drive* want `isMissionDir`, and only the
ones that resolve a mission *number* — `-proxy` (`proxy.go:629`) and
`-renumber` (`renumber.go:38`) — want `isNumberedMission`. Rather than change
the predicate at six call sites and leave six copies of the same walk, the walk
itself became one helper: `missionDirs` (`organise.go:540`) reads a year
directory and returns its mission slugs sorted, and `-checksum`, `-check` (both
the hot and the cold pass), `-verify`, `-index` and the flag store all go
through it. An enumeration that disagrees with the others is now a change to
one function rather than a predicate someone forgot to update.

`-index` was worth the separate look the finding asked for. `missionNum`
(`index.go:97`) returns 0 for a `000_*` slug, which is honest, and the URL
scheme turned out not to care — `indexhtml.go` keys clips and stills by
`year/slug/rel` throughout, never by number. The sort did care: it was a
`sort.Slice` on `Num` alone, which is not stable, so two `000_*` missions came
out in arbitrary order. It now breaks ties on the slug.

Regression test in `organise_test.go` on `missionDirs` against a real temporary
year directory: it asserts `000_Edits` comes back alongside the numbered
missions while `_unsorted`, `proxies` and a plain file that parses as a mission
name do not. It fails on the old predicate.

Left alone: the three listing sites in `status.go` still inline the same walk,
because each does more with the `DirEntry` than take its name and routing them
through `missionDirs` would mean re-`Stat`ing. They already use `isMissionDir`,
so they agree with the helper; they just do not share it. Also left: `slugNum`
(`renumber.go:136`) and `missionNum` (`index.go:97`) are still two more copies
of the parse, both called on already-filtered slugs.

### `-list` and `-status` showed any directory under the year as a mission

`runStatus` (`status.go:84`) and `runList` (`status.go:406`) accepted every
directory under the year directory as a mission row, so `_unsorted` — which
`-organise` creates for files whose date it could not resolve — was listed
alongside real missions, with a size and a per-drive presence marker, as were
any other strays. `runListAll` (`status.go:298`) already filtered, with
`isNumberedMission`, so the same `-list` disagreed with itself between
`-year 2026` and `-year all`.

Reusing `isNumberedMission` for the two unfiltered sites would have traded one
wrong row for a missing one: it requires `n > 0`, so it drops `000_Edits`, and
README.md is explicit that `000_*` directories are "synced like any mission" and
only unaddressable *by mission number*. Listing is not addressing. So the
predicate was split instead: `isMissionDir` (`organise.go:529`) accepts any
`NNN_` prefix including `000_`, `isNumberedMission` keeps the `n > 0` rule for
callers that resolve a mission number, and both now share one parse. All three
listing sites use `isMissionDir`, which also puts `000_*` back into
`-list -year all` where it belonged.

Regression test in `organise_test.go` on the two predicates rather than on the
listings: `runStatus` and `runList` print straight to stdout from real drives,
so the rows themselves are not assertable without splitting them up first.

Left alone at the time: the other `isNumberedMission` callers, and the gap that
left is the `000_*` finding above — a separate change with its own blast radius,
since it decides what `-checksum` writes and what `-check` compares.
Also left: `slugNum` (`renumber.go:136`) and `missionNum` (`index.go:97`) are
still two more copies of the same parse, differing only in what they return for
a non-mission name. Both are called on already-filtered slugs, so neither is
wrong today.

### An interrupt during the verify phase was ignored by `-sync` and `-replicate`

Both gated the copy phase with `if ctx.Err() != nil { select {} }` after
`p1.Wait()`, handing control to the interrupt handler, and both then ran the
verify phase with no equivalent gate after `p2.Wait()`. The per-file `ctx.Err()`
checks inside the verify workers only stopped hashes that had not started, so an
interrupt there fell through to writing `checksums.b3` and printing the success
summary — `✓ N synced to M archive(s)` — while the handler was still blocked on
stdin asking whether to delete the partial directories. Answering `y` then
deleted what the run had just reported as synced.

The manifest was the durable half of the damage. Only the files whose verify
happened to finish before the cancel land in `checksums`, and `mergeChecksums`
merges them into any existing `checksums.b3`, so an interrupted sync left a
manifest that agreed with a partial directory: a later `-evict` reads that
manifest as a full accounting of the cold copy, and `-check` compares against it.

Fixed by adding the gate after `p2.Wait()` in both files, matching `pull.go:365`
which already had it on both phases — this was an inconsistency between the
three, not a design decision. `replicate.go`'s bare `select {}` on the copy
phase picked up the same comment as the other three sites while there.

No regression test: `runSync` and `runReplicate` are single ~390-line functions
that scan real drives, print and prompt, so the interrupt path is not reachable
from a unit test without splitting them up first — the same reason the hot-drive
naming fix below has none.

Left alone: the gate stops the goroutine, it does not undo the copies in
flight. Those are already safe by the `.qcp-part-` rename below, so an interrupt
leaves whole files or nothing, and the handler's offer to delete the directories
it created is what covers the rest.

### An interrupted copy left a truncated file that the re-run then skipped

`job` (`copy.go:39`) wrote straight to the destination path, so the only cleanup
was the `os.Remove(dst)` on its own error returns. When the SIGINT handler
called `os.Exit(130)`, every copy in flight died mid-write and left a short file
behind at the final name. The `ctx.Err()` guards in the workers only stopped
copies that had not started.

The re-run could not tell. `missingByDst` (`main.go:656`) decides what to copy
with a bare `os.Stat`, and the sync side compares `findFiles` listings, so a
truncated file counted as present and was never re-copied or verified. It
survived into `checksums.b3` the next time `-checksum` ran, at which point the
manifest agreed with the corrupt bytes. Reachable from either answer to the
delete prompt — `n` keeps the partial mission by design — and from the
no-mission-in-flight path at `main.go:589`, which exits without prompting at
all.

Fixed by writing every copy to `partPath(dst)` — the destination name with
`.qcp-part-` in front, in the destination's own directory so the rename is
within one filesystem — and renaming only once `Sync`, `Close` and `Chmod` have
returned. An interrupt now leaves either nothing or a whole file at the
destination name, which is what makes "already present" a safe answer to "does
this still need copying". Threading a context into `job` was rejected as the
fix: it narrows the window rather than closing it, since the process can still
die between the last write and the removal.

The leading dot is doing real work. `findFiles` and `scanUnorganised` both skip
any path component starting with one, so a leftover temporary is invisible to
`-checksum`, `-sync`, `-list` and `-reorganise` rather than being taken for
footage — the same trick the flags file and the proxy lock already use.
Reclaiming the bytes is a separate matter, so `sweepCopyParts` clears the
mission directories a run is about to copy into, before any copy starts, and
prints what it cleared; `.qcp-part` is now a shared constant with the ffmpeg
temporaries in `proxy.go`, which `sweepPartFiles` already collected the same
way. Regression tests in `copy_test.go`: a fifo source pins the timing, so the
mid-copy assertion that nothing sits at the destination name is deterministic
rather than a race, and it fails on the old code.

Left alone: the sweep covers only the missions a run touches, so a mission
abandoned and never copied into again keeps its hidden leftovers. Sweeping the
whole drive on every run would mean walking the entire archive to reclaim a
handful of files, and would be unsafe besides — nothing locks a footage tree, so
a wider sweep could take a concurrent run's work in progress.

### The ingest interrupt state was shared with the signal handler unguarded

`intrDstRoots` and `intrIsNew` were plain variables in `runIngest`, written by
the main goroutine before each day's copy and read by the SIGINT handler on its
own goroutine with nothing between them. The handler decides from that pair
whether to offer to delete the partial mission and hand the mission number back,
so a torn slice header or a stale `isNew` picks the wrong mission to remove or
reverts a counter that was never minted. Never observed — `go test -race` never
reached it, because the interrupt path had no test — but real regardless.

Fixed by moving the pair into an `interruptTarget` (`main.go:160`) whose `set`,
`clear` and `get` all take one mutex. `get` returns both fields under the same
lock, so the handler cannot pair one mission's roots with another's `isNew`, and
it is called once at the top of the handler: what the prompt offers to delete is
the mission that was in flight when the signal arrived.

The second half of the finding was the clearing. `runDay` reset the pair only
inside `if !proxyOff`, so with `-proxy=false` it stayed pointing at a mission
that was by then copied *and verified* — a Ctrl-C in the window before the next
day started offered to delete good footage. `intr.clear()` now runs
unconditionally once the verify phase and the manifest write are done, with the
proxy comment split from it: the footage being safe is why the target is
cleared, and proxies being cheap to regenerate is why the tier that follows is
not guarded.

Regression test in `main_test.go`; the setter alternates missions whose `isNew`
is derivable from their roots, so without the mutex `-race` reports the race and
a torn pair fails the check independently.

Left alone: the handler still calls `os.Exit(130)` while the copy pools are
live, so copies in flight die mid-write. Reading that path is what turned up
finding 1 above — the partial files they leave are indistinguishable from
complete ones on the next run — but that is a fix to the copy path, not to the
interrupt state.

### A look edited in place never reached a proxy

`lookTransform` (`colour.go:244`) derived both the transform ID (`"look/" +
base`) and the cache filename (`"look_" + safe + ".cube"`) from the look's
basename alone. Editing `My Look.cube` in place therefore left the ID unchanged,
so nothing was marked stale by the `meta.Transform != transforms[i].ID` check at
`proxy.go:745`, *and* left the cache entry unchanged, so `ensureLUT` went on
returning the old cube it had copied in. The change reached nothing by either
route, contradicting README.md's promise that changing the look rebuilds the
affected proxies. Two looks sharing a filename in different directories collided
the same way, despite the comment claiming they could not.

Fixed by folding a blake3 hash of the cube's contents into both names:
`look/<name>@<hash>` and `look_<safe>_<hash>.cube`. An in-place edit is now
stale by construction and the collision is impossible, with the look's own name
kept in both so a proxy tree still says which look it was. `lookTransform` reads
the file, so it now returns an error: `runProxy` resolves the look once before
planning and exits on failure, `runIngestProxies` warns and skips the tier
rather than silently baking the technical conversion instead, and `planMission`
takes the resolved transform rather than a path — one reading per run, and a bad
look surfaces before any work starts. Regression tests in `colour_test.go`;
with the hash fixed to a constant, the edit test fails on both the ID and the
cache entry and the collision test fails too.

Left alone: old cubes accumulate in `proxies/luts/` under their old hashes.
Each is a few hundred KB, nothing else in the tree is garbage-collected either,
and keeping them is what lets an old proxy be traced to the exact bytes baked
into it.

### `-sync` named every hot drive by `volume`, so a `path`-only drive had none

`runSync` read `DriveConfig.Volume` directly in five places while every other
command went through `d.name()` (`config.go:62`), which falls back to the
basename of `path`. A drive configured with `path` and no `volume` — documented
in README.md for local directories — was therefore blank in the plan header, in
the scan-error and ghost lines, and on both sides of a `CONFLICT` message.

The empty string also became the `missionSource.srcVol`, and so the
`sourceLimiter` key. `sourceLimiter.add` returns early for a key it has already
seen (`pool.go:55`), so two `path`-only hot drives shared one semaphore and the
second silently inherited the first's probed worker count — an NVMe drive
throttled to an HDD's single reader, or the reverse.

Fixed by using `p.name()` at all five sites, matching the cold side, which
already called `dst.name()` at `sync.go:195`. The remaining direct `.Volume`
reads in the tree are all `CardConfig`, which has no `path`. No regression test:
`runSync` is one 390-line function that scans real drives, prints and prompts,
so the naming is not reachable from a unit test without splitting it up first.

Left alone: `sourceLimiter` still accepts an empty key. It cannot be handed one
now — `name()` returns a basename, and `basePath()` bottoms out at `/Volumes` —
and refusing the key would leave that drive *unlimited* rather than pooled,
which is the worse of the two failures.


### `-evict` deleted a hot copy it had never compared against the cold one

`qualifyBackups` (`evict.go:209`) cross-checked the two manifests with
`manifestConflicts(readChecksumFile(<hot>/checksums.b3), manifest)`.
`readChecksumFile` returns an empty map for a file that is missing or will not
parse, and `manifestConflicts` iterates the *reference* map, so a hot copy with
no manifest yielded zero conflicts and the cold copy qualified. The rest of the
bar still held — every hot file on the cold disk, in the cold manifest, and
re-read and hashed unless `-quick` — but nothing tied the cold bytes to the hot
ones. `-evict` is the only command that deletes data, so "I could not compare"
must refuse.

Fixed by reading every hot manifest once, up front, and returning the same
"run `-checksum NNN` first" note the cold side already produces at
`evict.go:173` when one is missing or unreadable. The conflict loop then walks
those maps instead of re-reading the file per cold drive. Regression test in
`evict_test.go`; without the guard a manifest-less hot copy qualifies.

Left alone: the hot manifest is still not required to *cover* every hot file, so
files added after the last `-checksum` — edit exports, say — are cross-checked
against nothing. `-sync` verified those on the way over, and requiring coverage
would make `-evict` refuse missions that are merely new rather than suspect.


### `-reorganise` moved `checksums.b3` into the new mission as if it were footage

`scanUnorganised` (`organise.go:238`) filtered dotfiles and `junkFiles` but
nothing excluded the manifest, so with `regroup=true` — where the walk descends
into existing numbered missions — it dated each mission's `checksums.b3` by
mtime and planned a move for it. With two or more source missions the plan's
collision rule renamed them, which put them out of reach of the stale-manifest
removal at `organise.go:439` (that only unlinks a file still called
`checksums.b3`), and they became permanent files inside the new mission:
`-checksum` hashed them, `-list` counted them, `-sync` carried them to cold
storage. One source mission happened to come out right, which is why it went
unnoticed.

Fixed by adding a `metadataFiles` set in `util.go` — `checksums.b3`,
`proxies.b3`, `proxies.json`, `.qcp-flags.json` — and skipping it in
`scanUnorganised` alongside `junkFiles`. The proxy files were only reachable if
a proxy tree ever moved under a year directory, and `.qcp-flags.json` was
already caught by the dotfile filter; naming all four in one place is what makes
the rule legible. Regression test in `organise_test.go`; without the guard the
scan returns six manifests as well as the two clips.


### `-ingest` hung forever when a hot drive was already up to date

`main.go:648` created a progress bar per destination unconditionally, including
destinations with nothing missing — the case the very next loop reports as
"already up to date". mpb reads `total <= 0` as "total unknown" and never fires
the complete event for such a bar, so `p1.Wait()` at `main.go:697` blocked
forever. It reproduced in the documented re-run, append and partially-mounted
scenarios.

Fixed in `progress.go` by calling `bar.EnableTriggerComplete()` when total is
zero, which covers every call site at once — `verify.go:106` could hang the same
way when a manifest listed only files that had gone from disk, and
`main.go:728`, `evict.go:300`, `index.go:288` and `proxy.go:1157` were reachable
with 0-byte inputs. `pull.go` was already immune because it filters zero-size
volumes out of `volOrder` before building bars. Regression test in
`progress_test.go`; it deadlocks on four of five cases without the guard.
