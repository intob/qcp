package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Resolve's scripting API is a Python (and Lua) module loaded out of the
// application bundle, with no C or HTTP surface a Go program can reach, so the
// bridge is a Python script handed the flag list on stdin. It is embedded
// rather than installed so there is one binary and nothing to keep in sync.
//
// Clips are matched on absolute source path: Resolve reports File Path exactly
// as qcp composes it (<drive>/<root>/<year>/<slug>/<rel>), so no filename
// guessing is involved and a clip that lives on two drives cannot be confused.
const resolveScript = `
import json, os, sys

api = "/Library/Application Support/Blackmagic Design/DaVinci Resolve/Developer/Scripting"
sys.path.append(os.path.join(api, "Modules"))
os.environ.setdefault("RESOLVE_SCRIPT_API", api)
os.environ.setdefault("RESOLVE_SCRIPT_LIB",
    "/Applications/DaVinci Resolve/DaVinci Resolve.app/Contents/Libraries/Fusion/fusionscript.so")

def fail(msg):
    print(json.dumps({"error": msg}))
    sys.exit(0)

try:
    import DaVinciResolveScript as dvr
except Exception as e:
    fail("cannot import the Resolve scripting module: %s" % e)

req = json.load(sys.stdin)
want = {c["Path"]: c["Colour"] for c in req.get("clips", [])}

resolve = dvr.scriptapp("Resolve")
if resolve is None:
    fail("Resolve is not running, or External Scripting is not enabled "
         "(Preferences > System > General > External scripting using: Local)")

project = resolve.GetProjectManager().GetCurrentProject()
if project is None:
    fail("no project is open in Resolve")

def walk(folder):
    items = list(folder.GetClipList())
    for sub in folder.GetSubFolderList():
        items += walk(sub)
    return items

items = walk(project.GetMediaPool().GetRootFolder())

applied, cleared, missing = [], [], []
seen = set()
for item in items:
    path = item.GetClipProperty("File Path")
    if not path:
        continue
    colour = want.get(path)
    if colour:
        seen.add(path)
        if colour not in (item.GetFlagList() or []):
            item.AddFlag(colour)
        if item.GetClipColor() != colour:
            item.SetClipColor(colour)
        applied.append(path)
    elif req.get("clear") and colour is None:
        # Only ever retract the colour qcp itself applies, so a flag or clip
        # colour set by hand in Resolve survives untouched.
        mine = req.get("colour")
        if mine in (item.GetFlagList() or []):
            item.ClearFlags(mine)
            if item.GetClipColor() == mine:
                item.ClearClipColor()
            cleared.append(path)

missing = [p for p in want if p not in seen]
print(json.dumps({
    "project": project.GetName(),
    "pool": len(items),
    "applied": applied,
    "cleared": cleared,
    "missing": missing,
}))
`

type resolveResult struct {
	Error   string   `json:"error"`
	Project string   `json:"project"`
	Pool    int      `json:"pool"`
	Applied []string `json:"applied"`
	Cleared []string `json:"cleared"`
	Missing []string `json:"missing"`
}

// runResolve pushes the flags recorded on the drives into the open Resolve
// project. It is deliberately one-way: the index is the source of truth, so
// there is no conflict to resolve and nothing in Resolve to read back.
func runResolve(cfg Config, clear bool) bool {
	store := newFlagStore(cfg)
	if len(store.drives) == 0 {
		exit(1, "no drives mounted")
	}
	clips := store.all()

	fmt.Printf("\n  %s  %s\n  %s  %d flagged clip(s) across %d mission(s)\n",
		dim("drives "), dim(strings.Join(driveNames(store.drives), ", ")),
		dim("flags  "), len(clips), countMissions(clips))
	if len(clips) == 0 && !clear {
		fmt.Printf("\n  nothing flagged — flag clips in the index under %s\n\n", bold("qcp -serve"))
		return true
	}

	payload, err := json.Marshal(map[string]any{
		"clips": clips, "clear": clear, "colour": flagColour,
	})
	if err != nil {
		exit(1, "err building the request: %v", err)
	}

	cmd := exec.Command("python3", "-c", resolveScript)
	cmd.Stdin = strings.NewReader(string(payload))
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		exit(1, "err running the Resolve bridge: %v", err)
	}
	var res resolveResult
	if err := json.Unmarshal(out, &res); err != nil {
		exit(1, "unexpected output from the Resolve bridge: %s", strings.TrimSpace(string(out)))
	}
	if res.Error != "" {
		fmt.Printf("\n  %s %s\n\n", red("✗"), res.Error)
		return false
	}

	fmt.Printf("  %s  %s  %s\n\n", dim("project"), bold(res.Project),
		dim(fmt.Sprintf("%d clip(s) in the media pool", res.Pool)))
	fmt.Printf("  %s  %d flagged\n", green("✓"), len(res.Applied))
	if len(res.Cleared) > 0 {
		fmt.Printf("  %s  %d unflagged\n", green("✓"), len(res.Cleared))
	}
	// A flagged clip that was never imported is the common case, not an error:
	// the index covers the whole archive and a project covers one shoot.
	if len(res.Missing) > 0 {
		fmt.Printf("  %s  %d not in this project\n", dim("·"), len(res.Missing))
		for _, p := range res.Missing[:min(len(res.Missing), 5)] {
			fmt.Printf("       %s\n", dim(filepath.Base(p)))
		}
		if len(res.Missing) > 5 {
			fmt.Printf("       %s\n", dim(fmt.Sprintf("and %d more", len(res.Missing)-5)))
		}
	}
	fmt.Println()
	return true
}

func countMissions(clips []flaggedClip) int {
	seen := map[string]bool{}
	for _, c := range clips {
		seen[fmt.Sprintf("%d/%s", c.Year, c.Slug)] = true
	}
	return len(seen)
}
