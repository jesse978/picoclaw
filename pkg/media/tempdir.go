package media

import (
	"os"
	"path/filepath"
)

const TempDirName = "picoclaw_media"

// TempDir returns the shared temporary directory used for downloaded media.
func TempDir() string {
	if dir := os.Getenv("PICOCLAW_MEDIA_DIR"); filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(os.TempDir(), TempDirName)
}
