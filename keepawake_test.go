package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// keepAwake's caffeinate was never stopped: a child is not killed with its
// parent on macOS, so every qcp run left one behind holding the Mac out of
// idle sleep — 382 of them over 47 days. It must end when qcp does.
func TestKeepAwakeEndsWithTheProcess(t *testing.T) {
	if _, err := exec.LookPath("caffeinate"); err != nil {
		t.Skip("caffeinate not available")
	}
	code, out := inSubprocess(t, t.TempDir(), func() {
		cmd := keepAwake()
		if cmd == nil {
			fmt.Println("pid=0")
			return
		}
		fmt.Printf("pid=%d\n", cmd.Process.Pid)
	})
	if code != 0 {
		t.Fatalf("child failed: %s", out)
	}
	i := strings.Index(out, "pid=")
	pid, _ := strconv.Atoi(strings.Fields(out[i+4:])[0])
	if pid == 0 {
		t.Fatal("caffeinate did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(pid, 0) != nil {
			return // gone
		}
		time.Sleep(50 * time.Millisecond)
	}
	syscall.Kill(pid, syscall.SIGTERM)
	t.Errorf("caffeinate %d outlived the process that started it", pid)
}
