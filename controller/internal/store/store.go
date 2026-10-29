package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const CapacityBytes int64 = 1500 * 1024 * 1024 * 1024

type Store struct {
	db *sql.DB
}

type Item struct {
	ID        string
	WlLo      int
	WlHi      int
	Chunk     int
	ItemIndex int
	Attempt   int
	State     string
}

type Progress struct {
	Total  int
	Done   int
	Failed int
	Leased int
	Bytes  int64
}

// Cell is one work item (a workload range inside a 6h chunk) and its lease state.
type Cell struct {
	Chunk int    `json:"chunk"`
	State string `json:"state"`
	WlLo  int    `json:"wlLo"`
	WlHi  int    `json:"wlHi"`
}

type Session struct {
	ID            string    `json:"id"`
	Dataset       string    `json:"dataset"`
	Name          string    `json:"name"`
	EndTime       string    `json:"endTime"`
	OffsetSeconds int64     `json:"offsetSeconds"`
	CreatedAt     time.Time `json:"createdAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
	LastAccess    time.Time `json:"lastAccess"`
	Closed        bool      `json:"closed"`
}

type ExportRow struct {
	Name        string `json:"name"`
	Dataset     string `json:"dataset"`
	Phase       string `json:"phase"`
	Type        string `json:"type"`
	SessionID   string `json:"sessionId,omitempty"`
	Anchor      string `json:"anchor,omitempty"`
	StartTime   string `json:"startTime"`
	EndTime     string `json:"endTime,omitempty"`
	Interval    string `json:"interval,omitempty"`
	Blocks      int    `json:"blocks"`
	BlocksTotal int    `json:"blocksTotal"`
	Bytes       int64  `json:"bytes"`
	Workers     int    `json:"workers,omitempty"`
	Heap        int64  `json:"heap,omitempty"`
	Budget      int64  `json:"budget,omitempty"`
	URL         string `json:"url"`
	StartedAt   int64  `json:"startedAt,omitempty"`
	FinishedAt  int64  `json:"finishedAt,omitempty"`
	CreatedAt   int64  `json:"createdAt,omitempty"`
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o775); err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.Join(dir, "state.db") + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS datasets (
  name TEXT PRIMARY KEY,
  phase TEXT NOT NULL,
  plan_digest TEXT,
  bytes INTEGER NOT NULL DEFAULT 0,
  reserved INTEGER NOT NULL DEFAULT 0,
  estimate INTEGER NOT NULL DEFAULT 0,
  parquet_deleted INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
  dataset TEXT NOT NULL,
  id TEXT NOT NULL,
  wl_lo INTEGER NOT NULL,
  wl_hi INTEGER NOT NULL,
  chunk INTEGER NOT NULL,
  item_index INTEGER NOT NULL,
  state TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  owner TEXT,
  expiry INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0,
  rows INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (dataset, id)
);
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  dataset TEXT NOT NULL,
  name TEXT,
  end_time TEXT NOT NULL,
  offset_seconds INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  last_access INTEGER NOT NULL,
  idle_seconds INTEGER NOT NULL,
  closed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS exports (
  name TEXT PRIMARY KEY,
  dataset TEXT NOT NULL,
  phase TEXT NOT NULL,
  start_time TEXT,
  blocks INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0,
  url TEXT,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  dataset TEXT,
  message TEXT
);`)
	if err != nil {
		return err
	}
	alters := []string{
		`ALTER TABLE exports ADD COLUMN type TEXT NOT NULL DEFAULT 'thanos'`,
		`ALTER TABLE exports ADD COLUMN session_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE exports ADD COLUMN anchor TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE exports ADD COLUMN end_time TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE exports ADD COLUMN interval TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE exports ADD COLUMN blocks_total INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE exports ADD COLUMN started_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE exports ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE exports ADD COLUMN workers INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE exports ADD COLUMN heap INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE exports ADD COLUMN budget INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE exports ADD COLUMN finished_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE datasets ADD COLUMN started_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE datasets ADD COLUMN finished_at INTEGER NOT NULL DEFAULT 0`,
	}
	for _, q := range alters {
		if _, err := s.db.Exec(q); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	if _, err := s.db.Exec(`UPDATE exports SET created_at=? WHERE created_at=0`, time.Now().Unix()); err != nil {
		return err
	}
	_, err = s.db.Exec(`CREATE TABLE IF NOT EXISTS export_blocks (
  export_name TEXT NOT NULL,
  id TEXT NOT NULL,
  chunk INTEGER NOT NULL,
  item_index INTEGER NOT NULL,
  wl_lo INTEGER NOT NULL,
  wl_hi INTEGER NOT NULL,
  step_lo INTEGER NOT NULL,
  step_hi INTEGER NOT NULL,
  emit_ns INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  owner TEXT,
  expiry INTEGER NOT NULL DEFAULT 0,
  attempt INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (export_name, id)
)`)
	return err
}

func (s *Store) UpsertDataset(name, phase, digest string, estimate, reserved int64) error {
	_, err := s.db.Exec(`INSERT INTO datasets(name, phase, plan_digest, estimate, reserved, updated_at)
VALUES(?,?,?,?,?,?)
ON CONFLICT(name) DO UPDATE SET phase=excluded.phase, plan_digest=excluded.plan_digest,
  estimate=excluded.estimate, reserved=excluded.reserved, updated_at=excluded.updated_at`,
		name, phase, digest, estimate, reserved, time.Now().Unix())
	return err
}

func (s *Store) SetPhase(name, phase string) error {
	_, err := s.db.Exec(`UPDATE datasets SET phase=?, updated_at=? WHERE name=?`, phase, time.Now().Unix(), name)
	return err
}

func (s *Store) Phase(name string) (string, error) {
	var phase string
	err := s.db.QueryRow(`SELECT phase FROM datasets WHERE name=?`, name).Scan(&phase)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return phase, err
}

func (s *Store) DatasetKnown(name string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM datasets WHERE name=?`, name).Scan(&n)
	return n > 0, err
}

func (s *Store) InsertItems(dataset string, items []Item) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, it := range items {
		_, err := tx.Exec(`INSERT OR IGNORE INTO items(dataset, id, wl_lo, wl_hi, chunk, item_index, state) VALUES(?,?,?,?,?,?, 'queued')`,
			dataset, it.ID, it.WlLo, it.WlHi, it.Chunk, it.ItemIndex)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ItemCount(dataset string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM items WHERE dataset=?`, dataset).Scan(&n)
	return n, err
}

func (s *Store) Lease(dataset, worker string, n int, ttl time.Duration) ([]Item, bool, error) {
	if n < 1 {
		n = 1
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	_, err = tx.Exec(`UPDATE items SET state='queued', owner=NULL WHERE dataset=? AND state='leased' AND expiry < ? AND expiry > 0`, dataset, now)
	if err != nil {
		return nil, false, err
	}
	rows, err := tx.Query(`SELECT id, wl_lo, wl_hi, chunk, item_index, attempt FROM items WHERE dataset=? AND state='queued' ORDER BY chunk, wl_lo LIMIT ?`, dataset, n)
	if err != nil {
		return nil, false, err
	}
	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ID, &it.WlLo, &it.WlHi, &it.Chunk, &it.ItemIndex, &it.Attempt); err != nil {
			rows.Close()
			return nil, false, err
		}
		items = append(items, it)
	}
	rows.Close()
	exp := time.Now().Add(ttl).Unix()
	for i := range items {
		items[i].Attempt++
		_, err = tx.Exec(`UPDATE items SET state='leased', owner=?, expiry=?, attempt=? WHERE dataset=? AND id=?`,
			worker, exp, items[i].Attempt, dataset, items[i].ID)
		if err != nil {
			return nil, false, err
		}
	}
	var left int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM items WHERE dataset=? AND state IN ('queued','leased')`, dataset).Scan(&left); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return items, left == 0, nil
}

func (s *Store) Heartbeat(dataset, worker string, ttl time.Duration) error {
	_, err := s.db.Exec(`UPDATE items SET expiry=? WHERE dataset=? AND owner=? AND state='leased'`,
		time.Now().Add(ttl).Unix(), dataset, worker)
	return err
}

func (s *Store) Complete(dataset, id string, bytes, rows int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE items SET state='done', bytes=?, rows=?, owner=NULL WHERE dataset=? AND id=? AND state!='done'`, bytes, rows, dataset, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		if _, err := tx.Exec(`UPDATE datasets SET bytes=bytes+?, updated_at=? WHERE name=?`, bytes, time.Now().Unix(), dataset); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Fail(dataset, id, worker, msg string, retryable bool) error {
	var attempt int
	err := s.db.QueryRow(`SELECT attempt FROM items WHERE dataset=? AND id=?`, dataset, id).Scan(&attempt)
	if err != nil {
		return err
	}
	state := "queued"
	if !retryable || attempt >= 3 {
		state = "failed"
	}
	_, err = s.db.Exec(`UPDATE items SET state=?, owner=NULL WHERE dataset=? AND id=?`, state, dataset, id)
	if err != nil {
		return err
	}
	_, _ = s.db.Exec(`INSERT INTO events(ts, dataset, message) VALUES(?,?,?)`, time.Now().Unix(), dataset, fmt.Sprintf("%s %s: %s", worker, id, msg))
	return nil
}

func (s *Store) Cells(dataset string) ([]Cell, error) {
	rows, err := s.db.Query(`SELECT chunk, state, wl_lo, wl_hi FROM items WHERE dataset=? ORDER BY chunk, item_index`, dataset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Cell
	for rows.Next() {
		var c Cell
		if err := rows.Scan(&c.Chunk, &c.State, &c.WlLo, &c.WlHi); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) Progress(dataset string) (Progress, error) {
	var p Progress
	err := s.db.QueryRow(`SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN state='done' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN state='leased' THEN 1 ELSE 0 END),0)
		FROM items WHERE dataset=?`, dataset).Scan(&p.Total, &p.Done, &p.Failed, &p.Leased)
	if err != nil {
		return p, err
	}
	_ = s.db.QueryRow(`SELECT bytes FROM datasets WHERE name=?`, dataset).Scan(&p.Bytes)
	return p, nil
}

func (s *Store) ReservedSum() (int64, error) {
	var n sql.NullInt64
	err := s.db.QueryRow(`SELECT COALESCE(SUM(reserved),0) FROM datasets`).Scan(&n)
	return n.Int64, err
}

func (s *Store) MarkParquetDeleted(dataset string) error {
	_, err := s.db.Exec(`UPDATE datasets SET parquet_deleted=1, bytes=0, reserved=0, updated_at=? WHERE name=?`, time.Now().Unix(), dataset)
	return err
}

func (s *Store) ParquetDeleted(dataset string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT parquet_deleted FROM datasets WHERE name=?`, dataset).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return n == 1, err
}

func (s *Store) DeleteDataset(name string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM items WHERE dataset=?`, name); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM sessions WHERE dataset=?`, name); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM datasets WHERE name=?`, name); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateSession(sess Session) error {
	_, err := s.db.Exec(`INSERT INTO sessions(id, dataset, name, end_time, offset_seconds, created_at, expires_at, last_access, idle_seconds, closed)
VALUES(?,?,?,?,?,?,?,?,?,0)`,
		sess.ID, sess.Dataset, sess.Name, sess.EndTime, sess.OffsetSeconds,
		sess.CreatedAt.Unix(), sess.ExpiresAt.Unix(), sess.LastAccess.Unix(), int64(sess.ExpiresAt.Sub(sess.CreatedAt).Seconds()))
	return err
}

func (s *Store) TouchSession(id string) error {
	_, err := s.db.Exec(`UPDATE sessions SET last_access=? WHERE id=? AND closed=0`, time.Now().Unix(), id)
	return err
}

func (s *Store) GetSession(id string) (Session, error) {
	var sess Session
	var created, expires, last int64
	var closed int
	err := s.db.QueryRow(`SELECT id, dataset, COALESCE(name,''), end_time, offset_seconds, created_at, expires_at, last_access, closed FROM sessions WHERE id=?`, id).
		Scan(&sess.ID, &sess.Dataset, &sess.Name, &sess.EndTime, &sess.OffsetSeconds, &created, &expires, &last, &closed)
	if err != nil {
		return sess, err
	}
	sess.CreatedAt = time.Unix(created, 0).UTC()
	sess.ExpiresAt = time.Unix(expires, 0).UTC()
	sess.LastAccess = time.Unix(last, 0).UTC()
	sess.Closed = closed == 1
	return sess, nil
}

func (s *Store) ListSessions(dataset string) ([]Session, error) {
	rows, err := s.db.Query(`SELECT id, dataset, COALESCE(name,''), end_time, offset_seconds, created_at, expires_at, last_access, closed FROM sessions WHERE dataset=? ORDER BY created_at`, dataset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		var sess Session
		var created, expires, last int64
		var closed int
		if err := rows.Scan(&sess.ID, &sess.Dataset, &sess.Name, &sess.EndTime, &sess.OffsetSeconds, &created, &expires, &last, &closed); err != nil {
			return nil, err
		}
		sess.CreatedAt = time.Unix(created, 0).UTC()
		sess.ExpiresAt = time.Unix(expires, 0).UTC()
		sess.LastAccess = time.Unix(last, 0).UTC()
		sess.Closed = closed == 1
		out = append(out, sess)
	}
	return out, rows.Err()
}

func (s *Store) LiveSessions(dataset string) ([]Session, error) {
	all, err := s.ListSessions(dataset)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var live []Session
	for _, s := range all {
		if !s.Closed && s.ExpiresAt.After(now) {
			live = append(live, s)
		}
	}
	return live, nil
}

func (s *Store) CloseSession(id string) error {
	_, err := s.db.Exec(`UPDATE sessions SET closed=1 WHERE id=?`, id)
	return err
}

func (s *Store) SaveExport(row ExportRow) error {
	if row.Type == "" {
		row.Type = "thanos"
	}
	created := row.CreatedAt
	if created == 0 {
		created = time.Now().Unix()
	}
	_, err := s.db.Exec(`INSERT INTO exports(name, dataset, phase, start_time, blocks, bytes, url, updated_at, type, session_id, anchor, end_time, interval, blocks_total, started_at, created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(name) DO UPDATE SET dataset=excluded.dataset, phase=excluded.phase, start_time=excluded.start_time,
  blocks=excluded.blocks, bytes=excluded.bytes, url=excluded.url, updated_at=excluded.updated_at,
  type=excluded.type, session_id=excluded.session_id, anchor=excluded.anchor, end_time=excluded.end_time,
  interval=excluded.interval, blocks_total=excluded.blocks_total, started_at=excluded.started_at,
  created_at=CASE WHEN exports.created_at=0 THEN excluded.created_at ELSE exports.created_at END`,
		row.Name, row.Dataset, row.Phase, row.StartTime, row.Blocks, row.Bytes, row.URL, time.Now().Unix(),
		row.Type, row.SessionID, row.Anchor, row.EndTime, row.Interval, row.BlocksTotal, row.StartedAt, created)
	return err
}

func (s *Store) TouchExport(name, phase string, blocks, total int, bytes int64, url string, startedAt int64) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(`UPDATE exports SET phase=?, blocks=?, blocks_total=?, bytes=?, url=CASE WHEN ?='' THEN url ELSE ? END, started_at=CASE WHEN ?>0 THEN ? ELSE started_at END, updated_at=? WHERE name=?`,
		phase, blocks, total, bytes, url, url, startedAt, startedAt, now, name)
	if err != nil {
		return err
	}
	if phase == "Building" {
		_, err = s.db.Exec(`UPDATE exports SET finished_at=0 WHERE name=?`, name)
		return err
	}
	if phase == "Serving" || phase == "Failed" {
		_, err = s.db.Exec(`UPDATE exports SET finished_at=? WHERE name=? AND finished_at=0 AND started_at>0`, now, name)
	}
	return err
}

func (s *Store) GenerationTimes(name string) (started, finished int64, err error) {
	err = s.db.QueryRow(`SELECT COALESCE(started_at,0), COALESCE(finished_at,0) FROM datasets WHERE name=?`, name).Scan(&started, &finished)
	if err == sql.ErrNoRows {
		return 0, 0, nil
	}
	return started, finished, err
}

func (s *Store) MarkGenerationStart(name string) error {
	_, err := s.db.Exec(`UPDATE datasets SET started_at=? WHERE name=? AND started_at=0`, time.Now().Unix(), name)
	return err
}

func (s *Store) MarkGenerationFinish(name string) error {
	_, err := s.db.Exec(`UPDATE datasets SET finished_at=? WHERE name=? AND finished_at=0 AND started_at>0`, time.Now().Unix(), name)
	return err
}

func (s *Store) TouchExportLive(name string, workers int, heap, budget int64) error {
	_, err := s.db.Exec(`UPDATE exports SET workers=?, heap=?, budget=?, updated_at=? WHERE name=?`,
		workers, heap, budget, time.Now().Unix(), name)
	return err
}

func (s *Store) UpsertExport(row ExportRow) error {
	return s.SaveExport(row)
}

func scanExport(row *ExportRow, scan func(dest ...any) error) error {
	return scan(&row.Name, &row.Dataset, &row.Phase, &row.StartTime, &row.Blocks, &row.Bytes, &row.URL,
		&row.Type, &row.SessionID, &row.Anchor, &row.EndTime, &row.Interval, &row.BlocksTotal, &row.StartedAt, &row.CreatedAt,
		&row.Workers, &row.Heap, &row.Budget, &row.FinishedAt)
}

const exportCols = `name, dataset, phase, COALESCE(start_time,''), blocks, bytes, COALESCE(url,''), COALESCE(type,'thanos'), COALESCE(session_id,''), COALESCE(anchor,''), COALESCE(end_time,''), COALESCE(interval,''), COALESCE(blocks_total,0), COALESCE(started_at,0), COALESCE(created_at,0), COALESCE(workers,0), COALESCE(heap,0), COALESCE(budget,0), COALESCE(finished_at,0)`

func (s *Store) GetExport(name string) (ExportRow, error) {
	var row ExportRow
	err := scanExport(&row, s.db.QueryRow(`SELECT `+exportCols+` FROM exports WHERE name=?`, name).Scan)
	return row, err
}

func (s *Store) ListExports() ([]ExportRow, error) {
	rows, err := s.db.Query(`SELECT ` + exportCols + ` FROM exports ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExportRow
	for rows.Next() {
		var row ExportRow
		if err := scanExport(&row, rows.Scan); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) DeleteExport(name string) error {
	_, err := s.db.Exec(`DELETE FROM exports WHERE name=?`, name)
	return err
}

type StorageDataset struct {
	Name     string `json:"name"`
	Bytes    int64  `json:"bytes"`
	Reserved int64  `json:"reserved"`
	Phase    string `json:"phase"`
}

func (s *Store) Storage() (used int64, datasets []StorageDataset, exports []ExportRow, err error) {
	rows, err := s.db.Query(`SELECT name, bytes, reserved, phase FROM datasets ORDER BY name`)
	if err != nil {
		return 0, nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var d StorageDataset
		if err := rows.Scan(&d.Name, &d.Bytes, &d.Reserved, &d.Phase); err != nil {
			return 0, nil, nil, err
		}
		used += d.Bytes
		datasets = append(datasets, d)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, nil, err
	}
	rows.Close()
	exports, err = s.ListExports()
	if err != nil {
		return 0, nil, nil, err
	}
	for _, e := range exports {
		used += e.Bytes
	}
	return used, datasets, exports, nil
}
