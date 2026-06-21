package main

import "os"

var colorEnabled = func() bool {
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}()

func bold(s string) string    { return sgr(s, "1") }
func dim(s string) string     { return sgr(s, "2") }
func red(s string) string     { return sgr(s, "31") }
func green(s string) string   { return sgr(s, "32") }
func yellow(s string) string  { return sgr(s, "33") }
func blue(s string) string    { return sgr(s, "34") }
func magenta(s string) string { return sgr(s, "35") }
func cyan(s string) string    { return sgr(s, "36") }

func sgr(s, code string) string {
	if !colorEnabled {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}
