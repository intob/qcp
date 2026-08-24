package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// runServe serves a built index over HTTP. The static site alone would open
// fine from file://, but the browse proxies live out on the drives, and a
// <video> pointed at file:///Volumes/... is at the mercy of the browser: an
// http:// page may not load file:// subresources at all, and macOS gates a
// browser's access to removable volumes behind a TCC prompt it never shows for
// a subresource. Serving the proxies from the same origin as the page sidesteps
// both — qcp already has access to the drives, so it does the reading.
func runServe(cfg Config, out, addr string) bool {
	out = indexOutDir(out)

	raw, err := os.ReadFile(filepath.Join(out, "index.json"))
	if err != nil {
		if os.IsNotExist(err) {
			exit(1, "no index at %s — run qcp -index first", out)
		}
		exit(1, "err reading index.json: %v", err)
	}
	var data indexData
	if err := json.Unmarshal(raw, &data); err != nil {
		exit(1, "err parsing index.json: %v", err)
	}

	// Every browse proxy the index knows about, by absolute path. This is the
	// whole allowlist: the media handler serves these files and nothing else,
	// so a crafted path cannot walk out of the proxy tree and read the disk.
	allowed := make(map[string]bool)
	// Which clips may be flagged, by the same principle: only what the index
	// published, so a crafted slug cannot steer a write outside the archive.
	allowedClips := make(map[string]bool)
	for _, y := range data.Years {
		for _, m := range y.Missions {
			for _, c := range m.Clips {
				allowedClips[flagKey(y.Year, m.Slug, c.Rel)] = true
			}
			if m.ProxyDir == "" {
				continue
			}
			for _, c := range m.Clips {
				if c.Browse != "" {
					allowed[filepath.Join(m.ProxyDir, c.Browse)] = true
				}
			}
		}
	}

	// Flags are read from and written to the drives on every request rather
	// than cached: the page is the only writer, but a mission can be flagged
	// from a second qcp -serve or edited between requests, and the file is a
	// few hundred bytes.
	flags := newFlagStore(cfg)

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir(out)))
	mux.HandleFunc("/api/flags", func(w http.ResponseWriter, r *http.Request) {
		out := map[string]bool{}
		for _, c := range flags.all() {
			out[flagKey(c.Year, c.Slug, c.Rel)] = true
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("/api/flag", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Year    int    `json:"year"`
			Slug    string `json:"slug"`
			Rel     string `json:"rel"`
			Flagged bool   `json:"flagged"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		// The slug and rel land in a filesystem path, so they have to be exactly
		// what the index published — never a path the caller invented.
		if !allowedClips[flagKey(req.Year, req.Slug, req.Rel)] {
			http.Error(w, "unknown clip", http.StatusNotFound)
			return
		}
		if _, err := flags.set(req.Year, req.Slug, req.Rel, req.Flagged); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]bool{"flagged": req.Flagged})
	})
	mux.HandleFunc("/media/", func(w http.ResponseWriter, r *http.Request) {
		p := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/media"))
		if !allowed[p] {
			http.NotFound(w, r)
			return
		}
		f, err := os.Open(p)
		if err != nil {
			// The index is a snapshot; the drive may since have been unplugged.
			http.Error(w, "proxy not reachable — is the drive mounted?", http.StatusServiceUnavailable)
			return
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			http.Error(w, "proxy not readable", http.StatusServiceUnavailable)
			return
		}
		// ServeContent handles Range itself, which is what lets the player seek.
		http.ServeContent(w, r, fi.Name(), fi.ModTime(), f)
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		exit(1, "err listening on %s: %v", addr, err)
	}

	missions, clips, size := data.counts()
	fmt.Printf("\n  %s  %s\n  %s  %s\n  %s  %d mission(s), %d clip(s), %s\n",
		dim("out    "), bold(out),
		dim("proxies"), dim(fmt.Sprintf("%d playable", len(allowed))),
		dim("index  "), missions, clips, fmtSize(uint64(size)))
	for _, u := range serveURLs(ln.Addr()) {
		fmt.Printf("\n  %s  %s", green("→"), bold(u))
	}
	fmt.Printf("\n\n%s\n", dim("ctrl-c to stop"))

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if err := srv.Serve(ln); err != nil {
		exit(1, "err serving: %v", err)
	}
	return true
}

// serveURLs lists the addresses the index is reachable on. Binding to all
// interfaces is the point of -addr :8080 — browsing the archive from a phone —
// so the LAN address is worth printing rather than making the user find it.
func serveURLs(a net.Addr) []string {
	tcp, ok := a.(*net.TCPAddr)
	if !ok {
		return []string{"http://" + a.String() + "/"}
	}
	port := fmt.Sprintf("%d", tcp.Port)
	if !tcp.IP.IsUnspecified() {
		return []string{"http://" + net.JoinHostPort(tcp.IP.String(), port) + "/"}
	}
	urls := []string{"http://" + net.JoinHostPort("localhost", port) + "/"}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return urls
	}
	for _, ia := range addrs {
		n, ok := ia.(*net.IPNet)
		if !ok || n.IP.IsLoopback() || n.IP.To4() == nil {
			continue
		}
		urls = append(urls, "http://"+net.JoinHostPort(n.IP.String(), port)+"/")
	}
	return urls
}

// flagKey identifies a clip across the index, the API and the drives.
func flagKey(year int, slug, rel string) string {
	return strconv.Itoa(year) + "/" + slug + "/" + rel
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
	}
}
