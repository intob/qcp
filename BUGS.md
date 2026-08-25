# Open bugs

Findings from a read of the whole tree on 2026-08-25. `go build`, `go vet` and
`go test ./...` are all clean, so none of these are caught by the existing
suite. Line numbers are against commit `d7d6706` plus the progress-bar fix.

Findings that have since been fixed are recorded at the bottom for context —
they explain why `progress.go`, the `metadataFiles` set in `util.go`, the hot
manifest read in `evict.go`, the `name()` calls in `sync.go`, the hashed look
IDs in `colour.go`, the `interruptTarget` in `main.go`, the `.qcp-part-`
temporaries in `copy.go`, the verify-phase gates in `sync.go`, `replicate.go`
and `main.go` and the `isMissionDir` predicate in `organise.go` look the way
they do.

Ordered by severity. Each entry says what is wrong, how it was confirmed, and
what a fix would have to do; none of the open ones have been attempted.

---

## 1. `000_*` missions are never hashed, checked or verified

`checksum.go:64`, `check.go:341`, `check.go:358`, `verify.go:180`,
`index.go:180`, `flags.go:211`

Noticed while fixing the listing filter below. README.md says `000_*`
directories "are synced like any mission but cannot be addressed by
mission-number commands", and `-sync` (`sync.go:56`) does carry them: it takes
every directory under the year that is not the proxy tree. But every command
that enumerates missions for itself filters with `isNumberedMission`, which
requires `n > 0`, so `000_Edits` gets no `checksums.b3` from `-checksum` (both
the year-wide walk and a targeted `-checksum NNN`, which cannot name it), is
not compared by `-check`, not re-hashed by `-verify`, not indexed, and its
flags are not collected. `-sync` writes the copies; nothing ever checks them.

**Fix direction.** Split the same way `-list` and `-status` were split below:
the commands that operate on *whatever is on the drive* want `isMissionDir`,
and only the ones that resolve a mission *number* — `-proxy`
(`proxy.go:629`) and `-renumber` (`renumber.go:38`) — want
`isNumberedMission`. Worth checking `-index` separately: `missionNum`
(`index.go:97`) returns 0 for a `000_*` slug, which is honest, but the sort and
the URL scheme may assume numbers are unique.

---

## Fixed

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

Left alone: the other `isNumberedMission` callers were not touched, and the gap
that leaves is now finding 1 above — a separate change with its own blast
radius, since it decides what `-checksum` writes and what `-check` compares.
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
