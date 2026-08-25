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

---

## Fixed

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
