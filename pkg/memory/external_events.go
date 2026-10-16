package memory

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/providers"
)

// AddMessageOnce records a delivered external event exactly once in the local
// transcript. The content marker recovers an append preceding a metadata write;
// metadata retains deduplication after compaction and explicit history clears.
func (s *JSONLStore) AddMessageOnce(_ context.Context, key, eventID string, msg providers.Message) error {
	if eventID == "" || strings.ContainsAny(eventID, "\r\n") {
		return fmt.Errorf("invalid external event ID")
	}
	l := s.sessionLock(key)
	l.Lock()
	defer l.Unlock()
	meta, err := s.readMeta(key)
	if err != nil {
		return err
	}
	if meta.ExternalEvents[eventID] {
		return nil
	}
	history, err := readMessages(s.jsonlPath(key), 0)
	if err != nil {
		return err
	}
	marker := "[External event: " + eventID + "]\n"
	found := false
	for _, old := range history {
		if strings.HasPrefix(old.Content, marker) {
			found = true
			break
		}
	}
	if !found {
		msg.Content = marker + msg.Content
		if err := s.addMsgLocked(key, msg); err != nil {
			return err
		}
		meta, err = s.readMeta(key)
		if err != nil {
			return err
		}
	}
	if meta.ExternalEvents == nil {
		meta.ExternalEvents = make(map[string]bool)
	}
	meta.ExternalEvents[eventID] = true
	return s.writeMeta(key, meta)
}
