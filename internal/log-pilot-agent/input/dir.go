package input

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"
	"time"
)

// DirConfig configures a directory-scanning Input.
type DirConfig struct {
	// Dir is the directory to watch for log files.
	Dir string `yaml:"dir"`
	// MetaPath is where per-file offsets are persisted (JSON).
	MetaPath string `yaml:"metaPath"`
	// ReadFrom controls where to start on first open: "newest" or "oldest".
	ReadFrom string `yaml:"readFrom"`
	// Include is a list of regex patterns; only matching filenames are read.
	Include []string `yaml:"include"`
	// Exclude is a list of regex patterns; matching filenames are skipped.
	Exclude []string `yaml:"exclude"`
	// OffsetCommitEvery persists state every N records (default 20000).
	OffsetCommitEvery int `yaml:"offsetCommitEvery"`
}

type dirOffsetState struct {
	CurrentFile string           `json:"currentFile"`
	Offset      int64            `json:"offset"`
	DoneInodes  map[string]int64 `json:"doneInodes"` // inode (uint64 as string) -> last offset
}

type dirInput struct {
	cfg          DirConfig
	currentF     *os.File
	currentFile  string
	currentInode uint64
	offset       int64
	reader       *bufio.Reader // bufio.Reader re-tries on EOF; Scanner does not
	doneInodes   map[string]int64
	includeRe    []interface{ MatchString(string) bool }
	excludeRe    []interface{ MatchString(string) bool }
	stopped      int32 // atomic
	lag          int64 // atomic
	readCount    int
}

// NewDirInput creates an Input that reads log files from a directory in
// modification-time order, handling log rotation via inode tracking.
//
// Key design decisions (matching EMReader from logexporter):
//   - Uses bufio.Reader not bufio.Scanner: Reader.ReadString returns "" on EOF
//     but allows retrying when the file grows, Scanner permanently fails after EOF.
//   - Offset tracks actual bytes consumed (not estimated from line length).
//   - detectRotation clears currentFile so advanceFile picks up same-named new file.
//   - inodeKey uses os.Stat to avoid extra file descriptors.
func NewDirInput(cfg DirConfig) (Input, error) {
	if cfg.OffsetCommitEvery == 0 {
		cfg.OffsetCommitEvery = 20000
	}
	d := &dirInput{
		cfg:        cfg,
		doneInodes: make(map[string]int64),
		includeRe:  compilePatternInterfaces(cfg.Include),
		excludeRe:  compilePatternInterfaces(cfg.Exclude),
	}
	d.loadState()
	return d, nil
}

func (d *dirInput) ReadBatch(ctx context.Context, size int) ([]Record, error) {
	if atomic.LoadInt32(&d.stopped) > 0 {
		return nil, nil
	}

	var records []Record
	// Use a 100ms window: collect what's available, then return.
	// Callers should sleep on empty batch (runner adds 200ms backoff).
	deadline := time.Now().Add(100 * time.Millisecond)

	for len(records) < size {
		select {
		case <-ctx.Done():
			return records, nil
		default:
		}
		if time.Now().After(deadline) {
			break
		}

		line, n, err := d.readLine()
		if n > 0 {
			d.offset += int64(n)
			d.updateLag()
		}
		if line != "" {
			records = append(records, Record{Data: []byte(line)})
			d.readCount++
		}
		if err == io.EOF {
			// No new data: try to advance to next file.
			if advanced := d.advanceFile(); !advanced {
				break // nothing to do; caller will back off
			}
		} else if err != nil {
			break
		}
	}
	return records, nil
}

func (d *dirInput) Lag() int64 { return atomic.LoadInt64(&d.lag) }

func (d *dirInput) Commit() error {
	d.commitState()
	return nil
}

func (d *dirInput) Close() error {
	atomic.StoreInt32(&d.stopped, 1)
	d.commitState()
	if d.currentF != nil {
		err := d.currentF.Close()
		d.currentF = nil
		d.reader = nil
		return err
	}
	return nil
}

// readLine reads one line from the current file using bufio.Reader.
// Returns (line, bytesConsumed, error).
// On EOF: returns ("", 0, io.EOF) — caller should retry after new data arrives
// or advance to the next file.
func (d *dirInput) readLine() (string, int, error) {
	if d.currentF == nil {
		if err := d.openFile(); err != nil {
			return "", 0, io.EOF
		}
	}

	// Check rotation before reading: inode change means the file was swapped.
	if d.detectRotation() {
		d.markCurrentDone()
		d.currentF.Close()
		d.currentF = nil
		d.reader = nil
		d.currentFile = "" // must clear so advanceFile picks up new same-name file
		d.offset = 0
		return "", 0, io.EOF
	}

	// bufio.Reader.ReadString returns partial data + io.EOF on truncated last line,
	// or full line + nil when delimiter found. Unlike Scanner, it doesn't die on EOF.
	raw, err := d.reader.ReadString('\n')
	n := len(raw)

	if n > 0 {
		// Strip trailing newline characters (\r\n or \n).
		line := raw
		for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
			line = line[:len(line)-1]
		}
		if err == nil || err == io.EOF {
			// Return the line even if err==io.EOF (last line without trailing newline).
			return line, n, nil
		}
		return line, n, err
	}
	// n == 0: no data available
	return "", 0, err
}

// openFile opens the file at d.currentFile (or the appropriate starting file
// if currentFile is empty), seeking to the saved offset.
func (d *dirInput) openFile() error {
	var target string
	var targetOffset int64

	if d.currentFile != "" {
		target = d.currentFile
		targetOffset = d.offset
	} else {
		files := d.listFiles()
		if len(files) == 0 {
			return fmt.Errorf("no files in dir %s", d.cfg.Dir)
		}
		if d.cfg.ReadFrom == "newest" {
			target = files[len(files)-1]
			targetOffset = -1 // seek to end
		} else {
			target = files[0]
			targetOffset = 0
		}
	}

	f, err := os.Open(target)
	if err != nil {
		// File may have been rotated away; clear so next call finds a fresh file.
		d.currentFile = ""
		return err
	}

	if targetOffset < 0 {
		off, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			f.Close()
			return err
		}
		targetOffset = off
	} else if targetOffset > 0 {
		if _, err := f.Seek(targetOffset, io.SeekStart); err != nil {
			// File may have been truncated; start from beginning.
			f.Seek(0, io.SeekStart)
			targetOffset = 0
		}
	}

	d.currentF = f
	d.currentFile = target
	d.offset = targetOffset
	d.currentInode = getInodeFromFile(f)
	d.reader = bufio.NewReaderSize(f, 1024*1024) // 1 MB buffer: handles long lines
	d.updateLag()
	return nil
}

// advanceFile looks for the next unread file in the directory.
// Returns true if a new file was opened.
func (d *dirInput) advanceFile() bool {
	files := d.listFiles()
	if len(files) == 0 {
		return false
	}

	// Mark the current file as done before trying to open the next one.
	// currentFile may be empty if detectRotation cleared it.
	currentBase := ""
	if d.currentFile != "" {
		currentBase = filepath.Base(d.currentFile)
	}

	for _, path := range files {
		base := filepath.Base(path)
		inode, err := inodeFromPath(path)
		if err != nil {
			continue // file disappeared
		}
		key := fmt.Sprintf("%d", inode)
		// Skip files already fully read (keyed by inode, so rename doesn't fool us).
		if _, done := d.doneInodes[key]; done {
			continue
		}
		// Skip the exact file we're currently reading.
		// After rotation currentFile is "", so this only fires for the un-rotated case.
		if base == currentBase && d.currentFile == path {
			continue
		}

		// Found a new file: mark current as done, open the new one.
		if d.currentF != nil {
			d.markCurrentDone()
			d.currentF.Close()
			d.currentF = nil
			d.reader = nil
		}
		d.currentFile = path
		d.offset = 0
		if err := d.openFile(); err == nil {
			return true
		}
	}
	return false
}

// detectRotation returns true if the current file has been rotated away
// (inode changed or file shrank). Called once per readLine, but protected
// by the 100ms deadline in ReadBatch, so effectively at most ~N times/100ms.
func (d *dirInput) detectRotation() bool {
	if d.currentF == nil || d.currentFile == "" {
		return false
	}
	fi, err := os.Stat(d.currentFile)
	if err != nil {
		return true // file gone
	}
	newInode := getInodeFromStat(fi)
	if newInode != 0 && d.currentInode != 0 && newInode != d.currentInode {
		return true // inode changed: file was replaced
	}
	if fi.Size() < d.offset {
		return true // file was truncated
	}
	return false
}

func (d *dirInput) markCurrentDone() {
	// Key on inode (not path): after rotation the file may be renamed,
	// but the inode stays the same — so we correctly skip it.
	if d.currentInode != 0 {
		key := fmt.Sprintf("%d", d.currentInode)
		d.doneInodes[key] = d.offset
	}
}

// inodeFromPath returns the inode number for a file path using os.Stat.
// Returns an error if the file can't be stat-ed (e.g. deleted during rotation).
func inodeFromPath(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return getInodeFromStat(fi), nil
}

// listFiles returns files in d.Dir matching include/exclude filters,
// sorted by modification time (oldest first).
func (d *dirInput) listFiles() []string {
	entries, err := os.ReadDir(d.cfg.Dir)
	if err != nil {
		return nil
	}

	type fe struct {
		path    string
		modTime time.Time
	}
	var files []fe

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !d.passesFilter(name) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fe{
			path:    filepath.Join(d.cfg.Dir, name),
			modTime: info.ModTime(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})

	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.path
	}
	return paths
}

func (d *dirInput) passesFilter(name string) bool {
	if len(d.includeRe) > 0 {
		matched := false
		for _, re := range d.includeRe {
			if re.MatchString(name) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, re := range d.excludeRe {
		if re.MatchString(name) {
			return false
		}
	}
	return true
}

func (d *dirInput) updateLag() {
	if d.currentF == nil {
		return
	}
	info, err := d.currentF.Stat()
	if err != nil {
		return
	}
	remaining := info.Size() - d.offset
	if remaining < 0 {
		remaining = 0
	}
	atomic.StoreInt64(&d.lag, remaining)
}

// commitState atomically writes current read state to MetaPath.
func (d *dirInput) commitState() {
	d.updateLag()
	if d.cfg.MetaPath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(d.cfg.MetaPath), 0755); err != nil {
		return
	}
	state := dirOffsetState{
		CurrentFile: d.currentFile,
		Offset:      d.offset,
		DoneInodes:  d.doneInodes,
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	tmp := d.cfg.MetaPath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, d.cfg.MetaPath) // atomic on POSIX
}

func (d *dirInput) loadState() {
	if d.cfg.MetaPath == "" {
		return
	}
	raw, err := os.ReadFile(d.cfg.MetaPath)
	if err != nil {
		return
	}
	var state dirOffsetState
	if err := json.Unmarshal(raw, &state); err != nil {
		return
	}
	d.currentFile = state.CurrentFile
	d.offset = state.Offset
	if state.DoneInodes != nil {
		d.doneInodes = state.DoneInodes
	}
}

// getInodeFromFile returns the inode number of an open file.
func getInodeFromFile(f *os.File) uint64 {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &stat); err != nil {
		return 0
	}
	return stat.Ino
}

// getInodeFromStat returns the inode number from an os.FileInfo.
func getInodeFromStat(fi os.FileInfo) uint64 {
	if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
		return sys.Ino
	}
	return 0
}

// compilePatternInterfaces wraps compilePatterns result for use with the passesFilter method.
func compilePatternInterfaces(patterns []string) []interface{ MatchString(string) bool } {
	res := compilePatterns(patterns)
	out := make([]interface{ MatchString(string) bool }, len(res))
	for i, re := range res {
		out[i] = re
	}
	return out
}
