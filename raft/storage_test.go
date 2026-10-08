package raft

import (
	"os"
	"testing"
)

func mkEntries(from, to, term uint64) []Entry {
	var es []Entry
	for i := from; i <= to; i++ {
		es = append(es, Entry{Term: term, Index: i, Data: []byte{byte(i)}})
	}
	return es
}

func TestHardStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if term, vote, err := loadHardState(dir); err != nil || term != 0 || vote != "" {
		t.Fatalf("fresh dir: %d %q %v", term, vote, err)
	}
	if err := saveHardState(dir, 7, "n2"); err != nil {
		t.Fatal(err)
	}
	term, vote, err := loadHardState(dir)
	if err != nil || term != 7 || vote != "n2" {
		t.Fatalf("got %d %q %v", term, vote, err)
	}
	saveHardState(dir, 8, "")
	if term, vote, _ := loadHardState(dir); term != 8 || vote != "" {
		t.Fatalf("got %d %q", term, vote)
	}
}

func TestLogAppendReload(t *testing.T) {
	dir := t.TempDir()
	l, err := openLog(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.append(mkEntries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	l.append([]Entry{{Term: 2, Index: 6}}) // no-op entry (nil data)
	if err := l.append(mkEntries(9, 9, 2)); err == nil {
		t.Fatal("gap must be rejected")
	}
	l.close()

	l, err = openLog(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	if l.lastIndex() != 6 || l.lastTerm() != 2 {
		t.Fatalf("last = %d/%d", l.lastIndex(), l.lastTerm())
	}
	if tm, ok := l.term(3); !ok || tm != 1 {
		t.Fatalf("term(3) = %d %v", tm, ok)
	}
	if l.entries[5].Data != nil {
		t.Fatal("no-op must reload as nil data")
	}
}

func TestLogTruncateCompactAndTornTail(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(dir, true)
	l.append(mkEntries(1, 10, 1))
	if err := l.truncateFrom(8); err != nil {
		t.Fatal(err)
	}
	l.append(mkEntries(8, 9, 2))
	if tm, _ := l.term(8); tm != 2 {
		t.Fatalf("term(8) = %d", tm)
	}
	if err := l.compactTo(4); err != nil {
		t.Fatal(err)
	}
	if l.prevIndex != 4 || l.entries[0].Index != 5 {
		t.Fatalf("compaction window wrong: prev=%d first=%d", l.prevIndex, l.entries[0].Index)
	}
	if _, ok := l.term(3); ok {
		t.Fatal("compacted term must be unknown")
	}
	l.close()

	// Tear the last record in half.
	st, _ := os.Stat(logPath(dir))
	os.Truncate(logPath(dir), st.Size()-5)
	l, err := openLog(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	if l.lastIndex() != 8 {
		t.Fatalf("after torn tail last = %d, want 8", l.lastIndex())
	}
	if err := l.append(mkEntries(9, 9, 3)); err != nil {
		t.Fatal(err)
	}
}

func TestLogRewriteAndReset(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(dir, true)
	l.append(mkEntries(1, 3000, 1))
	if err := l.compactTo(2500); err != nil { // triggers physical rewrite
		t.Fatal(err)
	}
	if l.dropped != 0 {
		t.Fatalf("expected rewrite, dropped=%d", l.dropped)
	}
	l.append(mkEntries(3001, 3002, 1))
	l.close()
	l, _ = openLog(dir, true)
	if l.prevIndex != 2500 || l.lastIndex() != 3002 {
		t.Fatalf("reload after rewrite: prev=%d last=%d", l.prevIndex, l.lastIndex())
	}
	if err := l.reset(5000, 9); err != nil {
		t.Fatal(err)
	}
	l.close()
	l, _ = openLog(dir, true)
	defer l.close()
	if l.prevIndex != 5000 || l.prevTerm != 9 || l.lastIndex() != 5000 {
		t.Fatalf("after reset: prev=%d/%d last=%d", l.prevIndex, l.prevTerm, l.lastIndex())
	}
}

func TestTermLookups(t *testing.T) {
	dir := t.TempDir()
	l, _ := openLog(dir, true)
	defer l.close()
	l.append(mkEntries(1, 3, 1))
	l.append(mkEntries(4, 6, 2))
	l.append(mkEntries(7, 8, 4))
	if l.lastIndexOfTerm(2) != 6 || l.firstIndexOfTerm(2) != 4 || l.lastIndexOfTerm(3) != 0 {
		t.Fatalf("term lookups wrong: %d %d %d", l.lastIndexOfTerm(2), l.firstIndexOfTerm(2), l.lastIndexOfTerm(3))
	}
}
