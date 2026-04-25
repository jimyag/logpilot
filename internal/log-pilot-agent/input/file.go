package input

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"

	"github.com/nxadm/tail"
)

// FileConfig configures a file-tailing Input.
type FileConfig struct {
	// Path is the absolute path to the log file.
	Path string `yaml:"path"`
	// MetaPath is the directory for persisting read offsets.
	MetaPath string `yaml:"metaPath"`
	// ReadFrom controls the starting position: "newest" (default) or "oldest".
	ReadFrom string `yaml:"readFrom"`
	// Include is a list of regex patterns; only matching filenames are tailed.
	// Empty means all files pass.
	Include []string `yaml:"include"`
	// Exclude is a list of regex patterns; matching filenames are skipped.
	Exclude []string `yaml:"exclude"`
	// OffsetCommitEvery persists the read offset every N records.
	OffsetCommitEvery int `yaml:"offsetCommitEvery"`
}

type fileInput struct {
	cfg       FileConfig
	tail      *tail.Tail
	stopped   int32 // atomic
	lag       int64 // atomic, approximate bytes remaining
	readCount int
	includeRe []*regexp.Regexp
	excludeRe []*regexp.Regexp
}

// NewFileInput creates a file-tailing Input.
// metaDir is the directory where offset state will be persisted.
func NewFileInput(cfg FileConfig, metaDir string) (Input, error) {
	if cfg.OffsetCommitEvery == 0 {
		cfg.OffsetCommitEvery = 20000
	}
	if cfg.MetaPath == "" && metaDir != "" {
		cfg.MetaPath = filepath.Join(metaDir, filepath.Base(cfg.Path)+".offset")
	}

	seekInfo := &tail.SeekInfo{Offset: 0, Whence: 2} // newest by default
	if cfg.ReadFrom == "oldest" {
		seekInfo = &tail.SeekInfo{Offset: 0, Whence: 0}
	}

	t, err := tail.TailFile(cfg.Path, tail.Config{
		Follow:    true,
		ReOpen:    true, // follow across log rotation
		MustExist: false,
		Location:  seekInfo,
		Logger:    tail.DiscardingLogger,
	})
	if err != nil {
		return nil, err
	}

	fi := &fileInput{
		cfg:       cfg,
		tail:      t,
		includeRe: compilePatterns(cfg.Include),
		excludeRe: compilePatterns(cfg.Exclude),
	}

	if info, err := os.Stat(cfg.Path); err == nil {
		atomic.StoreInt64(&fi.lag, info.Size())
	}

	return fi, nil
}

func (f *fileInput) ReadBatch(ctx context.Context, size int) ([]Record, error) {
	if atomic.LoadInt32(&f.stopped) > 0 {
		return nil, nil
	}

	var records []Record
	for len(records) < size {
		select {
		case line, ok := <-f.tail.Lines:
			if !ok {
				return records, nil
			}
			if line.Err != nil {
				continue
			}
			if !f.passesFilter(filepath.Base(f.cfg.Path)) {
				continue
			}
			records = append(records, Record{Data: []byte(line.Text)})
			f.readCount++
			if f.readCount%f.cfg.OffsetCommitEvery == 0 {
				f.commitOffset()
			}
		case <-ctx.Done():
			return records, nil
		default:
			if len(records) > 0 {
				return records, nil
			}
			// No records buffered yet; block until one arrives or context expires.
			select {
			case line, ok := <-f.tail.Lines:
				if !ok {
					return records, nil
				}
				if line.Err == nil && f.passesFilter(filepath.Base(f.cfg.Path)) {
					records = append(records, Record{Data: []byte(line.Text)})
					f.readCount++
				}
			case <-ctx.Done():
				return records, nil
			}
		}
	}
	return records, nil
}

func (f *fileInput) Lag() int64 { return atomic.LoadInt64(&f.lag) }

func (f *fileInput) Close() error {
	atomic.StoreInt32(&f.stopped, 1)
	f.commitOffset()
	return f.tail.Stop()
}

func (f *fileInput) commitOffset() {
	if info, err := os.Stat(f.cfg.Path); err == nil {
		pos, _ := f.tail.Tell()
		remaining := info.Size() - pos
		if remaining < 0 {
			remaining = 0
		}
		atomic.StoreInt64(&f.lag, remaining)
	}
	// TODO: persist offset to f.cfg.MetaPath for crash recovery
}

func (f *fileInput) passesFilter(filename string) bool {
	if len(f.includeRe) > 0 {
		matched := false
		for _, re := range f.includeRe {
			if re.MatchString(filename) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, re := range f.excludeRe {
		if re.MatchString(filename) {
			return false
		}
	}
	return true
}

func compilePatterns(patterns []string) []*regexp.Regexp {
	var res []*regexp.Regexp
	for _, p := range patterns {
		if re, err := regexp.Compile(p); err == nil {
			res = append(res, re)
		}
	}
	return res
}
