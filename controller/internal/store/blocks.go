package store

import (
	"time"
)

// Block is one 2h TSDB block a converter worker can claim.
type Block struct {
	ID      string `json:"id"`
	Chunk   int    `json:"chunk"`
	Index   int    `json:"index"`
	WlLo    int    `json:"wlLo"`
	WlHi    int    `json:"wlHi"`
	StepLo  int    `json:"stepLo"`
	StepHi  int    `json:"stepHi"`
	EmitNS  bool   `json:"emitNS"`
	State   string `json:"state"`
	Bytes   int64  `json:"bytes,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
}

type BlockProgress struct {
	Total, Done, Failed, Leased int
	Bytes                       int64
}

func (s *Store) InsertBlocks(export string, blocks []Block) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, b := range blocks {
		emit := 0
		if b.EmitNS {
			emit = 1
		}
		_, err := tx.Exec(`INSERT OR IGNORE INTO export_blocks(export_name, id, chunk, item_index, wl_lo, wl_hi, step_lo, step_hi, emit_ns, state)
VALUES(?,?,?,?,?,?,?,?,?, 'queued')`,
			export, b.ID, b.Chunk, b.Index, b.WlLo, b.WlHi, b.StepLo, b.StepHi, emit)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListBlocks(export string) ([]Block, error) {
	rows, err := s.db.Query(`SELECT id, chunk, item_index, wl_lo, wl_hi, step_lo, step_hi, emit_ns, state, bytes
FROM export_blocks WHERE export_name=? ORDER BY step_lo, chunk, item_index`, export)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Block
	for rows.Next() {
		var b Block
		var emit int
		if err := rows.Scan(&b.ID, &b.Chunk, &b.Index, &b.WlLo, &b.WlHi, &b.StepLo, &b.StepHi, &emit, &b.State, &b.Bytes); err != nil {
			return nil, err
		}
		b.EmitNS = emit == 1
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) BlockProgress(export string) (BlockProgress, error) {
	var p BlockProgress
	err := s.db.QueryRow(`SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN state='done' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN state='leased' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(bytes),0)
		FROM export_blocks WHERE export_name=?`, export).Scan(&p.Total, &p.Done, &p.Failed, &p.Leased, &p.Bytes)
	return p, err
}

// RequeueBlocks returns leased blocks to the queue. Used when the converter
// job is gone and nobody is still writing them.
func (s *Store) RequeueBlocks(export string) error {
	_, err := s.db.Exec(`UPDATE export_blocks SET state='queued', owner=NULL, expiry=0 WHERE export_name=? AND state='leased'`, export)
	return err
}

func (s *Store) LeaseBlocks(export, worker string, n int, ttl time.Duration) ([]Block, bool, error) {
	if n < 1 {
		n = 1
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	if _, err := tx.Exec(`UPDATE export_blocks SET state='queued', owner=NULL, expiry=0 WHERE export_name=? AND state='leased' AND expiry > 0 AND expiry < ?`, export, now); err != nil {
		return nil, false, err
	}
	rows, err := tx.Query(`SELECT id, chunk, item_index, wl_lo, wl_hi, step_lo, step_hi, emit_ns, attempt FROM export_blocks
WHERE export_name=? AND state='queued' ORDER BY step_lo, chunk, item_index LIMIT ?`, export, n)
	if err != nil {
		return nil, false, err
	}
	var items []Block
	for rows.Next() {
		var b Block
		var emit int
		if err := rows.Scan(&b.ID, &b.Chunk, &b.Index, &b.WlLo, &b.WlHi, &b.StepLo, &b.StepHi, &emit, &b.Attempt); err != nil {
			rows.Close()
			return nil, false, err
		}
		b.EmitNS = emit == 1
		b.State = "leased"
		items = append(items, b)
	}
	rows.Close()
	exp := time.Now().Add(ttl).Unix()
	for i := range items {
		items[i].Attempt++
		if _, err := tx.Exec(`UPDATE export_blocks SET state='leased', owner=?, expiry=?, attempt=? WHERE export_name=? AND id=?`,
			worker, exp, items[i].Attempt, export, items[i].ID); err != nil {
			return nil, false, err
		}
	}
	var left int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM export_blocks WHERE export_name=? AND state IN ('queued','leased')`, export).Scan(&left); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return items, left == 0, nil
}

func (s *Store) HeartbeatBlocks(export, worker string, ttl time.Duration) error {
	_, err := s.db.Exec(`UPDATE export_blocks SET expiry=? WHERE export_name=? AND owner=? AND state='leased'`,
		time.Now().Add(ttl).Unix(), export, worker)
	return err
}

func (s *Store) CompleteBlock(export, id string, bytes int64) error {
	_, err := s.db.Exec(`UPDATE export_blocks SET state='done', bytes=?, owner=NULL, expiry=0 WHERE export_name=? AND id=? AND state!='done'`,
		bytes, export, id)
	return err
}

func (s *Store) FailBlock(export, id string, retryable bool) error {
	var attempt int
	if err := s.db.QueryRow(`SELECT attempt FROM export_blocks WHERE export_name=? AND id=?`, export, id).Scan(&attempt); err != nil {
		return err
	}
	state := "queued"
	if !retryable || attempt >= 3 {
		state = "failed"
	}
	_, err := s.db.Exec(`UPDATE export_blocks SET state=?, owner=NULL, expiry=0 WHERE export_name=? AND id=?`, state, export, id)
	return err
}

func (s *Store) DeleteBlocks(export string) error {
	_, err := s.db.Exec(`DELETE FROM export_blocks WHERE export_name=?`, export)
	return err
}

func (s *Store) Generating() (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM datasets WHERE phase='Generating'`).Scan(&n)
	return n > 0, err
}
