//go:build !darwin

package main

import "os"

// keepOutOfCache is a no-op where F_NOCACHE does not exist; see
// nocache_darwin.go.
func keepOutOfCache(f *os.File) error { return nil }
