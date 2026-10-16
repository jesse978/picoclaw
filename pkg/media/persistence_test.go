package media

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMediaRefsSurviveRestartAndRespectFileOwnership(t *testing.T) {
	dir := t.TempDir()
	cfg := MediaCleanerConfig{StateDir: filepath.Join(dir, "index"), MaxAge: 7 * 24 * time.Hour}
	s := NewFileMediaStoreWithCleanup(cfg)
	photo := createTempFile(t, dir, "photo.jpg")
	doc := createTempFile(t, dir, "user-file.txt")
	ref, err := s.Store(photo, MediaMeta{Filename: "photo.jpg"}, "chat")
	if err != nil {
		t.Fatal(err)
	}
	keep, err := s.Store(doc, MediaMeta{CleanupPolicy: CleanupPolicyForgetOnly}, "chat")
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewFileMediaStoreWithCleanup(cfg)
	if got, err := restarted.Resolve(ref); err != nil || got != photo {
		t.Fatalf("lost photo after restart: %q %v", got, err)
	}
	if err := restarted.ReleaseAll("chat"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(photo); !os.IsNotExist(err) {
		t.Fatal("owned temporary file was not removed")
	}
	if _, err := os.Stat(doc); err != nil {
		t.Fatal("user-owned file was removed", err)
	}
	again := NewFileMediaStoreWithCleanup(cfg)
	for _, r := range []string{ref, keep} {
		if _, err := again.Resolve(r); err == nil {
			t.Fatal("released ref reappeared after restart")
		}
	}
}

func TestMediaStoreFailsIfReferenceCannotBePersisted(t *testing.T) {
	dir := t.TempDir()
	blocked := createTempFile(t, dir, "not-a-directory")
	s := NewFileMediaStoreWithCleanup(MediaCleanerConfig{StateDir: blocked})
	file := createTempFile(t, dir, "photo.jpg")
	if _, err := s.Store(file, MediaMeta{}, "chat"); err == nil {
		t.Fatal("expected persistence error")
	}
	if len(s.refs) != 0 {
		t.Fatal("unrecoverable ref was published")
	}
}

func TestPersistedMediaExpiryUsesOriginalStoredTime(t *testing.T) {
	dir := t.TempDir()
	cfg := MediaCleanerConfig{StateDir: filepath.Join(dir, "index"), MaxAge: 7 * 24 * time.Hour}
	s := NewFileMediaStoreWithCleanup(cfg)
	s.nowFunc = func() time.Time { return time.Now().Add(-8 * 24 * time.Hour) }
	file := createTempFile(t, dir, "expired.jpg")
	ref, err := s.Store(file, MediaMeta{}, "chat")
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewFileMediaStoreWithCleanup(cfg)
	if got := restarted.CleanExpired(); got != 1 {
		t.Fatalf("expired refs removed=%d", got)
	}
	if _, err := restarted.Resolve(ref); err == nil {
		t.Fatal("expired ref survived restart")
	}
}
