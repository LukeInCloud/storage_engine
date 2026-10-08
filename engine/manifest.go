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
//	log N          WAL files numbered < N are fully flushed to SSTables
//	table LEVEL ID SSTables per level; within level 0 oldest first (newer
//	               files shadow older ones), in other levels any order
//
// It is replaced atomically (write temp, fsync, rename, fsync dir).
type manifest struct {
	logNum uint64
	levels [numLevels][]uint64
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
	for l, ids := range m.levels {
		for _, id := range ids {
			fmt.Fprintf(w, "table %d %d\n", l, id)
		}
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
		f := strings.Fields(line)
		bad := fmt.Errorf("manifest: bad line %q", line)
		switch {
		case len(f) == 2 && f[0] == "log":
			n, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return m, bad
			}
			m.logNum = n
		case len(f) == 3 && f[0] == "table":
			l, err1 := strconv.Atoi(f[1])
			id, err2 := strconv.ParseUint(f[2], 10, 64)
			if err1 != nil || err2 != nil || l < 0 || l >= numLevels {
				return m, bad
			}
			m.levels[l] = append(m.levels[l], id)
		default:
			return m, bad
		}
	}
	return m, nil
}
