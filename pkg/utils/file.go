package utils

import (
	"os"
	"path/filepath"
)

// DagReader interface for reading dag files
type DagReader interface {
	ReadPathsFromDir(dir string) ([]string, error)
	ReadDag(path string) ([]byte, error)
}

var (
	DefaultReader DagReader = &FileDagReader{}
)

// FileDagReader reads dag files from a directory
type FileDagReader struct {
}

// ReadPathsFromDir reads all dag files from a directory
func (r FileDagReader) ReadPathsFromDir(dir string) (dagFiles []string, err error) {
	if err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		ext := filepath.Ext(path)
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		dagFiles = append(dagFiles, path)
		return nil
	}); err != nil {
		return
	}

	return
}

// ReadDag reads a dag file
func (r FileDagReader) ReadDag(path string) ([]byte, error) {
	return os.ReadFile(path)
}
