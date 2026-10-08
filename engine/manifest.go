package engine

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// manifest is the single source of truth about which files make up the DB.
//
//	log N    WAL files numbered < N are fully flushed to SSTables
//	table ID SSTables, oldest first (later tables shadow earlier ones)
//
// It is replaced atomically (write temp, fsync, rename, fsync dir).
type manifest struct {
	logNum uint64
	tables []uint64
}

const manifestName = "MANIFEST"

func writeManifest(dir string, m manifest) error {
	tmp := filepath.Join(dir, manifestName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "log %d\n", m.logNum)
	for _, id := range m.tables {
		fmt.Fprintf(w, "table %d\n", id)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		return err
	}
	return syncDir(dir)
}

func readManifest(dir string) (manifest, error) {
	var m manifest
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		kind, val, ok := strings.Cut(line, " ")
		n, perr := strconv.ParseUint(val, 10, 64)
		if !ok || perr != nil {
			return m, fmt.Errorf("manifest: bad line %q", line)
		}
		switch kind {
		case "log":
			m.logNum = n
		case "table":
			m.tables = append(m.tables, n)
		default:
			return m, fmt.Errorf("manifest: bad line %q", line)
		}
	}
	return m, nil
}
