package main

import (
	"os"
	"os/exec"
	"testing"
)

// The commands report failure with os.Exit, which would take the test runner
// down with it. A test that expects one to exit builds its fixture in the
// parent, runs the command in a child copy of the test binary, and inspects
// what the child left on disk.
//
// The child is the same test re-run with QCP_SUBPROCESS naming it, so the test
// has to reach inSubprocess along the same path in both: fixtureDir gives the
// child the parent's directory, and setup the parent does is guarded by
// isChild.

func isChild(t *testing.T) bool { return os.Getenv("QCP_SUBPROCESS") == t.Name() }

func fixtureDir(t *testing.T) string {
	if isChild(t) {
		return os.Getenv("QCP_FIXTURE")
	}
	return t.TempDir()
}

// inSubprocess runs body in the child and returns its exit code and output.
// HOME is pointed into the fixture so nothing touches the real ~/.qcp_seq.
func inSubprocess(t *testing.T, dir string, body func()) (int, string) {
	t.Helper()
	if isChild(t) {
		body()
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), "QCP_SUBPROCESS="+t.Name(), "QCP_FIXTURE="+dir, "HOME="+dir)
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), string(out)
	}
	if err != nil {
		t.Fatal(err)
	}
	return 0, string(out)
}
