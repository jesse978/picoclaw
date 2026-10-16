package media

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sipeed/picoclaw/pkg/fileutil"
	"github.com/sipeed/picoclaw/pkg/logger"
)

type persistedMedia struct {
	Ref      string    `json:"ref"`
	Path     string    `json:"path"`
	Meta     MediaMeta `json:"meta"`
	Scope    string    `json:"scope"`
	StoredAt time.Time `json:"stored_at"`
}

func (s *FileMediaStore) refPath(ref string) string {
	return filepath.Join(s.cleanerCfg.StateDir, strings.TrimPrefix(ref, "media://")+".json")
}

func (s *FileMediaStore) persistRef(ref, path, scope string, meta MediaMeta, storedAt time.Time) error {
	if s.cleanerCfg.StateDir == "" {
		return nil
	}
	if err := os.MkdirAll(s.cleanerCfg.StateDir, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(persistedMedia{Ref: ref, Path: path, Scope: scope, Meta: meta, StoredAt: storedAt})
	if err != nil {
		return err
	}
	return fileutil.WriteFileAtomic(s.refPath(ref), data, 0o600)
}

func (s *FileMediaStore) restoreRefs() {
	if s.cleanerCfg.StateDir == "" {
		return
	}
	entries, err := os.ReadDir(s.cleanerCfg.StateDir)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		logger.WarnCF("media", "Cannot restore media references", map[string]any{"error": err.Error()})
		return
	}
	for _, f := range entries {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.cleanerCfg.StateDir, f.Name())
		data, err := os.ReadFile(path)
		var record persistedMedia
		if err == nil {
			err = json.Unmarshal(data, &record)
		}
		id := strings.TrimPrefix(record.Ref, "media://")
		if _, parseErr := uuid.Parse(id); parseErr != nil || record.Ref != "media://"+id || f.Name() != id+".json" {
			err = fmt.Errorf("invalid persisted media reference")
		}
		if err != nil {
			logger.WarnCF("media", "Cannot restore media reference", map[string]any{"file": f.Name(), "error": err.Error()})
			continue
		}
		if _, err := os.Stat(record.Path); err != nil {
			if os.IsNotExist(err) {
				_ = os.Remove(path)
			}
			continue
		}
		s.refs[record.Ref] = mediaEntry{path: record.Path, meta: record.Meta, storedAt: record.StoredAt}
		if s.scopeToRefs[record.Scope] == nil {
			s.scopeToRefs[record.Scope] = make(map[string]struct{})
		}
		s.scopeToRefs[record.Scope][record.Ref] = struct{}{}
		s.refToScope[record.Ref] = record.Scope
		s.refToPath[record.Ref] = record.Path
		state := s.pathStates[record.Path]
		if state.refCount == 0 {
			state.deleteEligible = normalizeCleanupPolicy(record.Meta.CleanupPolicy) == CleanupPolicyDeleteOnCleanup
		}
		if record.Meta.CleanupPolicy == CleanupPolicyForgetOnly {
			state.deleteEligible = false
		}
		state.refCount++
		s.pathStates[record.Path] = state
	}
}
