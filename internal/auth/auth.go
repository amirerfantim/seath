// Package auth provides HTTP Basic Auth backed by a recorder_users table
// (bcrypt-hashed passwords) in Postgres.
package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Roles. admin can do everything; viewer is read-only (GET requests plus the
// read-only SQL console).
const (
	RoleAdmin  = "admin"
	RoleViewer = "viewer"
)

const createUsersTable = `
CREATE TABLE IF NOT EXISTS recorder_users (
	username      TEXT PRIMARY KEY,
	password_hash TEXT NOT NULL,
	created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`

// alterAddRole upgrades pre-existing users tables. Existing users default to
// admin so an upgrade never locks anyone out.
const alterAddRole = `ALTER TABLE recorder_users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'admin';`

const createAuditTable = `
CREATE TABLE IF NOT EXISTS recorder_audit (
	id        BIGSERIAL PRIMARY KEY,
	ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	username  TEXT NOT NULL,
	method    TEXT NOT NULL,
	path      TEXT NOT NULL,
	status    INTEGER NOT NULL
);`

// alterAddAuditContext adds the per-recording context. Nullable: rows written
// before it existed are matched by path instead.
const alterAddAuditContext = `
ALTER TABLE recorder_audit
	ADD COLUMN IF NOT EXISTS recording_id TEXT,
	ADD COLUMN IF NOT EXISTS action       TEXT,
	ADD COLUMN IF NOT EXISTS detail       TEXT;`

const createAuditRecordingIdx = `
CREATE INDEX IF NOT EXISTS recorder_audit_recording_idx ON recorder_audit (recording_id, id DESC);`

// EnsureUsersTable creates the recorder_users + recorder_audit tables if needed.
func EnsureUsersTable(db *sql.DB) error {
	for _, q := range []string{createUsersTable, alterAddRole, createAuditTable, alterAddAuditContext, createAuditRecordingIdx} {
		if _, err := db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// User is a configured login with its role.
type User struct {
	Name string `json:"name"`
	Role string `json:"role"`
}

// AddUser inserts or updates a user with a bcrypt-hashed password and role.
func AddUser(db *sql.DB, username, password, role string) error {
	if username == "" || password == "" {
		return fmt.Errorf("username and password are required")
	}
	if role == "" {
		role = RoleAdmin
	}
	if role != RoleAdmin && role != RoleViewer {
		return fmt.Errorf("role must be %q or %q", RoleAdmin, RoleViewer)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	_, err = db.Exec(
		`INSERT INTO recorder_users (username, password_hash, role) VALUES ($1, $2, $3)
		 ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash, role = EXCLUDED.role`,
		username, string(hash), role)
	return err
}

// RemoveUser deletes a user. Returns whether a row was removed.
func RemoveUser(db *sql.DB, username string) (bool, error) {
	res, err := db.Exec(`DELETE FROM recorder_users WHERE username = $1`, username)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ListUsers returns all users with their roles.
func ListUsers(db *sql.DB) ([]User, error) {
	rows, err := db.Query(`SELECT username, role FROM recorder_users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.Name, &u.Role); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// AuditEntry is one recorded mutating action. RecordingID/Action/Detail are set
// only for actions a handler annotated (see AuditContext).
type AuditEntry struct {
	TS          time.Time `json:"ts"`
	Username    string    `json:"username"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	Status      int       `json:"status"`
	RecordingID string    `json:"recording_id,omitempty"`
	Action      string    `json:"action,omitempty"`
	Detail      string    `json:"detail,omitempty"`
}

const selectAudit = `SELECT ts, username, method, path, status, recording_id, action, detail FROM recorder_audit`

// ListAudit returns the most recent audit entries (newest first).
func ListAudit(db *sql.DB, limit int) ([]AuditEntry, error) {
	rows, err := db.Query(selectAudit+` ORDER BY id DESC LIMIT $1`, clampAuditLimit(limit))
	if err != nil {
		return nil, err
	}
	return scanAudit(rows)
}

// ListRecordingAudit returns the audit trail of one recording, newest first,
// falling back to path matching for rows written before recording_id existed.
func ListRecordingAudit(db *sql.DB, recordingID string, limit int) ([]AuditEntry, error) {
	if recordingID == "" {
		return nil, fmt.Errorf("recording id is required")
	}
	rows, err := db.Query(selectAudit+`
		WHERE recording_id = $1
		   OR (recording_id IS NULL AND path LIKE $2 ESCAPE '\')
		ORDER BY id DESC LIMIT $3`,
		recordingID, "/api/recordings/"+escapeLike(recordingID)+"%", clampAuditLimit(limit))
	if err != nil {
		return nil, err
	}
	return scanAudit(rows)
}

func clampAuditLimit(limit int) int {
	if limit <= 0 || limit > 500 {
		return 100
	}
	return limit
}

// escapeLike neutralises LIKE wildcards so an id is matched literally.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

func escapeLike(s string) string { return likeEscaper.Replace(s) }

func scanAudit(rows *sql.Rows) ([]AuditEntry, error) {
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var recID, action, detail sql.NullString
		if err := rows.Scan(&e.TS, &e.Username, &e.Method, &e.Path, &e.Status, &recID, &action, &detail); err != nil {
			return nil, err
		}
		e.RecordingID, e.Action, e.Detail = recID.String, action.String, detail.String
		out = append(out, e)
	}
	return out, rows.Err()
}

// CountUsers returns the number of configured users.
func CountUsers(db *sql.DB) (int, error) {
	var n int
	err := db.QueryRow(`SELECT count(*) FROM recorder_users`).Scan(&n)
	return n, err
}

// Authenticator provides cookie-session authentication. Login verifies the
// bcrypt password once and issues an opaque session token (stored in memory);
// each request resolves that token to a username and re-reads the live role,
// so role changes and user removals take effect immediately.
type Authenticator struct {
	db       *sql.DB
	mu       sync.Mutex
	sessions map[string]session // token -> session
}

type session struct {
	username string
	exp      time.Time
}

const (
	sessionCookie = "nr_session"
	sessionTTL    = 7 * 24 * time.Hour
)

// New builds an Authenticator and ensures the users/audit tables exist.
func New(db *sql.DB) (*Authenticator, error) {
	if err := EnsureUsersTable(db); err != nil {
		return nil, err
	}
	return &Authenticator{db: db, sessions: make(map[string]session)}, nil
}

// Middleware enforces authentication via session cookie, handles login/logout,
// and applies role-based authorization + audit logging.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Always-public: health probes, the login page + its assets, and the
		// login/logout endpoints themselves.
		switch {
		case path == "/healthz" || path == "/readyz":
			next.ServeHTTP(w, r)
			return
		case r.Method == http.MethodPost && path == "/api/login":
			a.handleLogin(w, r)
			return
		case r.Method == http.MethodPost && path == "/api/logout":
			a.handleLogout(w, r)
			return
		case path == "/login.html" || path == "/style.css" || path == "/favicon.ico" || path == "/favicon.svg" || strings.HasPrefix(path, "/fonts/"):
			next.ServeHTTP(w, r)
			return
		}

		user, role, ok := a.resolve(r)
		if !ok {
			if strings.HasPrefix(path, "/api/") {
				writeJSONError(w, http.StatusUnauthorized, "not authenticated")
			} else {
				http.Redirect(w, r, "/login.html", http.StatusFound)
			}
			return
		}

		// Authorization: viewers may only read (GET) or run the read-only SQL
		// console. Everything else is admin-only.
		if role != RoleAdmin && !(r.Method == http.MethodGet || (r.Method == http.MethodPost && path == "/api/query")) {
			writeJSONError(w, http.StatusForbidden, "this action requires an admin account")
			return
		}

		ctx := context.WithValue(r.Context(), userCtxKey, user)
		ctx = context.WithValue(ctx, roleCtxKey, role)
		r = r.WithContext(ctx)

		// Audit mutating requests (everything except plain GETs). The handler may
		// annotate the AuditContext; it is read back once the handler returns.
		if r.Method != http.MethodGet {
			ac := &AuditContext{}
			r = r.WithContext(context.WithValue(r.Context(), auditCtxKey, ac))
			sr := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sr, r)
			a.audit(user, r.Method, path, sr.status, ac)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// resolve validates the session cookie and returns the current username + role,
// re-reading the role from the DB so changes/removals apply immediately.
func (a *Authenticator) resolve(r *http.Request) (string, string, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return "", "", false
	}
	a.mu.Lock()
	s, ok := a.sessions[c.Value]
	if ok && time.Now().After(s.exp) {
		delete(a.sessions, c.Value)
		ok = false
	}
	a.mu.Unlock()
	if !ok {
		return "", "", false
	}
	var role string
	if err := a.db.QueryRow(`SELECT role FROM recorder_users WHERE username = $1`, s.username).Scan(&role); err != nil {
		// User removed (or backend error) — drop the session.
		a.mu.Lock()
		delete(a.sessions, c.Value)
		a.mu.Unlock()
		return "", "", false
	}
	return s.username, role, true
}

func (a *Authenticator) handleLogin(w http.ResponseWriter, r *http.Request) {
	// A native HTML form POST (urlencoded) lets the browser/OS password manager
	// offer to save credentials and redirects on success; JSON is for API/tests.
	isForm := !strings.Contains(r.Header.Get("Content-Type"), "application/json")
	var username, password string
	if isForm {
		username = r.PostFormValue("username")
		password = r.PostFormValue("password")
	} else {
		var body struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request")
			return
		}
		username, password = body.Username, body.Password
	}

	fail := func(status int, msg string) {
		if isForm {
			http.Redirect(w, r, "/login.html?error="+url.QueryEscape(msg), http.StatusSeeOther)
			return
		}
		writeJSONError(w, status, msg)
	}

	var hash, role string
	err := a.db.QueryRow(`SELECT password_hash, role FROM recorder_users WHERE username = $1`, username).Scan(&hash, &role)
	if err == sql.ErrNoRows {
		if n, _ := CountUsers(a.db); n == 0 {
			fail(http.StatusUnauthorized, "no users configured yet — create one with: seath user add <name>")
			return
		}
		fail(http.StatusUnauthorized, "invalid username or password")
		return
	}
	if err != nil {
		fail(http.StatusInternalServerError, "auth backend error")
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		fail(http.StatusUnauthorized, "invalid username or password")
		return
	}

	token := newToken()
	a.mu.Lock()
	a.sessions[token] = session{username: username, exp: time.Now().Add(sessionTTL)}
	a.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionTTL.Seconds()),
	})
	a.audit(username, "POST", "/api/login", http.StatusOK, nil)

	if isForm {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"user": username, "role": role})
}

func (a *Authenticator) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		a.mu.Lock()
		delete(a.sessions, c.Value)
		a.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
	w.WriteHeader(http.StatusNoContent)
}

func newToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

type ctxKey int

const (
	userCtxKey ctxKey = iota
	roleCtxKey
	auditCtxKey
)

// AuditContext lets a handler describe what a mutating request actually did.
// A handler that annotates nothing still produces the plain method/path entry.
type AuditContext struct {
	RecordingID string
	Action      string
	Detail      string
}

// Annotate records which recording a request acted on, what it did, and a short
// human-readable detail. A no-op on requests that are not audited.
func Annotate(ctx context.Context, recordingID, action, detail string) {
	ac, ok := ctx.Value(auditCtxKey).(*AuditContext)
	if !ok {
		return
	}
	ac.RecordingID, ac.Action, ac.Detail = recordingID, action, detail
}

// RoleFromContext returns the authenticated user's role (set by Middleware).
func RoleFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(roleCtxKey).(string); ok {
		return v
	}
	return ""
}

// UserFromContext returns the authenticated username (set by Middleware).
func UserFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(userCtxKey).(string); ok {
		return v
	}
	return ""
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (a *Authenticator) audit(username, method, path string, status int, ac *AuditContext) {
	var recID, action, detail sql.NullString
	if ac != nil {
		recID, action, detail = nullString(ac.RecordingID), nullString(ac.Action), nullString(ac.Detail)
	}
	if _, err := a.db.Exec(
		`INSERT INTO recorder_audit (username, method, path, status, recording_id, action, detail)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		username, method, path, status, recID, action, detail); err != nil {
		slog.Warn("audit insert failed", "err", err)
	}
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
