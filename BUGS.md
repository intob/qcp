# Open bugs

Findings from a read of the whole tree on 2026-08-25. `go build`, `go vet` and
`go test ./...` are all clean, so none of these are caught by the existing
suite. Line numbers are against commit `d7d6706` plus the progress-bar fix.

Two findings from that pass are already fixed and are recorded at the bottom for
context — they explain why `progress.go` and the `metadataFiles` set in `util.go`
look the way they do.

Ordered by severity. Each entry says what is wrong, how it was confirmed, and
what a fix would have to do; none of them have been attempted.

---

## 1. `-evict`'s manifest cross-check is vacuous when the hot copy has no manifest

`evict.go:209` — `qualifyBackups`

```go
conflicts += len(manifestConflicts(readChecksumFile(filepath.Join(t.dir, "checksums.b3")), manifest))
```

`readChecksumFile` returns an empty map for a file that is missing or will not
parse, and `manifestConflicts` iterates the *reference* map — so an absent hot
manifest yields zero conflicts and the cold copy qualifies.

The rest of the bar still holds (every hot file is on the cold disk, listed in
the cold manifest, and re-read and hashed unless `-quick`), but with no hot
manifest nothing ever ties the cold bytes to the hot ones. `-evict` is the only
command that deletes data, so "I could not compare" should refuse rather than
pass.

The same wording — "the two manifests must agree wherever both mention a file"
in README.md — means the check also degrades quietly for individual files: a
mission that had edit exports added after ingest has them in the cold manifest
(written by `-sync`) but not the hot one, so those files are cross-checked
against nothing. That case is less alarming because `-sync` verified them on the
way over.

**Fix direction.** Treat a missing or unreadable hot `checksums.b3` as a
qualification failure with the same "run `-checksum NNN` first" note the cold
side already produces at `evict.go:173`. Deciding whether to also require the
hot manifest to *cover* every hot file is a judgement call — it would make
`-evict` refuse missions with newly added, never-checksummed files.

---

## 2. `-sync` uses `DriveConfig.Volume` instead of `name()`

`sync.go:118, 123, 129, 136, 214`

Every other command goes through `d.name()` (`config.go:62`), which falls back
to the basename of `path` when `volume` is unset. `sync.go` is the only place
that reads `.Volume` directly.

A drive configured with `path` and no `volume` — documented in README.md for
local directories — therefore gets an empty name throughout `-sync`: blank in
the plan header, blank in the scan-error and ghost lines, and blank on both
sides of a `CONFLICT` message.

Worse, `missionSources` stores that empty string as `srcVol` (`sync.go:136`),
which becomes the `sourceLimiter` key at `sync.go:270`. `sourceLimiter.add`
returns early for a key it has already seen (`pool.go:55`), so two `path`-only
source drives share one semaphore and the second silently inherits the first's
probed worker count — an NVMe drive throttled to an HDD's single reader, or the
reverse.

**Fix direction.** Replace all five with `p.name()`. Consider whether
`sourceLimiter` should also refuse an empty key rather than silently pooling.

---

## 3. A look edited in place is never picked up

`colour.go:282` — `ensureLUT`, with `lookTransform` at `colour.go:244`

`lookTransform` derives both the transform ID (`"look/" + base`) and the cache
filename (`"look_" + safe + ".cube"`) from the look's **basename**. `ensureLUT`
then returns the cached cube whenever it exists and is non-empty.

So editing `My Look.cube` in place leaves the ID unchanged — nothing is marked
stale by the `meta.Transform != transforms[i].ID` check at `proxy.go:745` — *and*
keeps serving the old cached copy. The change never reaches a proxy, by either
route.

This contradicts README.md: "Changing or removing the look changes the transform
recorded per clip, which marks the affected proxies stale and rebuilds them as
you touch each mission." That holds only when the change is to a *different
file*.

Two different looks that share a filename in different directories collide the
same way, despite the comment at `colour.go:243` claiming they cannot.

**Fix direction.** Fold a hash of the cube's contents into the ID and the cached
filename. That makes an in-place edit stale by construction and makes the
collision impossible. Note the cache lives in the proxy tree, so old cubes will
accumulate under their old hashes — decide whether that matters.

---

## 4. Data race on the ingest interrupt state

`main.go:553-554`, written at `792`, `864-865`, `953-954`, read at `564`, `580`, `584`

`intrDstRoots` and `intrIsNew` are plain variables shared between the main
goroutine and the SIGINT handler goroutine with no synchronisation. The handler
decides from them whether to offer to delete the partial mission and revert the
counter.

Not observed in practice, and `go test -race` does not reach it because the
interrupt path has no test. The race is real regardless — the handler can read a
torn or stale slice header.

There is a related correctness wrinkle at `main.go:791-794`: the pair is cleared
to `nil, false` only when proxies run. With `-proxy=false` it stays pointing at
the just-completed, fully verified mission, so a Ctrl-C in the window after
`runDay` finishes offers to delete good footage.

**Fix direction.** A mutex, or move the state into a small struct behind one.
Clearing the pair unconditionally at the end of `runDay` fixes the second half.

---

## 5. Interrupt during the verify phase is not honoured in `-sync` / `-replicate`

`sync.go:336`, `replicate.go:317`

The copy phase bails out with `if ctx.Err() != nil { select {} }`, handing
control to the interrupt handler. The verify phase that follows has the per-file
`ctx.Err()` guards inside the workers but no equivalent gate after
`p2.Wait()` — so an interrupt there falls through to writing `checksums.b3` and
printing the success summary while the handler is still waiting on stdin for the
delete prompt.

`pull.go:280` and `pull.go:355` do have the gate on both phases, so this is an
inconsistency between the three, not a design decision.

**Fix direction.** Add the same gate after `p2.Wait()` in both files.

---

## 6. `-list` and `-status` do not filter to numbered missions

`status.go:84` (`runStatus`), `status.go:406` (`runList`)

Both accept any directory under the year directory as a mission. `runListAll`
(`status.go:298`) and `-check` (`check.go:341`) filter with `isNumberedMission`.
The result is that `_unsorted` — which `-organise` creates — and any other stray
directory appears as a mission row in `-list` and `-status` but not in
`-list -year all`.

**Fix direction.** Add the `isNumberedMission` guard to both, matching
`runListAll`. Worth confirming the intent for `000_*` directories first:
`isNumberedMission` rejects them (it requires `n > 0`), so adding the guard would
drop `000_Edits` from `-list` and `-status`, where it currently shows. README.md
says those directories "sort to the top and are synced like any mission but
cannot be addressed by mission-number commands", which suggests they *should*
stay visible in listings — in which case the predicate needs splitting rather
than reusing.

---

## Fixed

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
