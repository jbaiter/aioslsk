-- V0001: Initial schema for aioslsk database

-- Holds the history of applied schema migrations
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Contains shared directories, corresponding Python class is
-- shares.model.SharedDirectory
CREATE TABLE IF NOT EXISTS directories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory TEXT NOT NULL,
    absolute_path TEXT NOT NULL UNIQUE,
    alias TEXT NOT NULL UNIQUE,
    -- 0 = everyone, 1 = friends, 2 = users
    share_mode INTEGER NOT NULL CHECK(share_mode BETWEEN 0 AND 2),
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Mapping of directory_id to usernames that have access, if share_mode is 2 (users)
CREATE TABLE IF NOT EXISTS directory_users (
    directory_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    PRIMARY KEY (directory_id, username),
    FOREIGN KEY (directory_id) REFERENCES directories (id) ON DELETE CASCADE
);

-- Files contained in shared directories, corresponding Python class is shares.model.SharedItem
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory_id INTEGER NOT NULL,
    subdir TEXT NOT NULL,
    filename TEXT NOT NULL,
    remote_path TEXT GENERATED ALWAYS AS (subdir || '\' || filename) VIRTUAL,
    -- SQLite FTS5 doesn't support prefix wildcards, so we store a reversed version
    -- of the remote path to allow suffix wildcard searches for things like file extensions
    fts_path TEXT GENERATED ALWAYS AS (REVERSE(remote_path)) VIRTUAL,
    mtime REAL NOT NULL,
    size_bytes INTEGER NOT NULL,
    -- Encoded as `<int>:<value>` pairs separated by pipes
    attributes TEXT,
    FOREIGN KEY (directory_id) REFERENCES directories (id) ON DELETE CASCADE,
    UNIQUE (directory_id, subdir, filename)
);

-- Index for fast lookup of files by remote_path, used for transfers and share listings
CREATE INDEX IF NOT EXISTS idx_files_remote_path ON files (remote_path);

-- Full-Text Search virtual table for files, using FTS5 on the reversed remote path
-- to allow suffix wildcard searches.
-- We don't further customize tokenchars because the memory implementation
-- treats most special characters (including _ - ( ) etc.) as word separators
-- via the regex [\W_]. The default unicode61 tokenizer behavior matches this.
-- We only keep / as a tokenchar because directory paths use it.
CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
    fts_path,
    content='files',
    content_rowid='id',
    tokenize="unicode61 remove_diacritics 2 tokenchars '/'",
    prefix='3'
);

-- Contains prebaked and compressed share listing responses
CREATE TABLE IF NOT EXISTS prebaked_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- 0 = everyone, 1 = friends, 2 = users
    share_mode INTEGER NOT NULL CHECK(share_mode BETWEEN 0 AND 2) UNIQUE,
    listing BLOB NOT NULL,
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Triggers to keep FTS table in sync with files table
CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
  INSERT INTO files_fts(rowid, fts_path) VALUES (new.id, new.fts_path);
END;
CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
  INSERT INTO files_fts(files_fts, rowid, fts_path) VALUES('delete', old.id, old.fts_path);
END;
CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files
WHEN old.fts_path <> new.fts_path
BEGIN
  -- Deleting the old row from the FTS table is necessary to prevent stale
  -- entries that can cause incorrect search results and listing counts.
  INSERT INTO files_fts(files_fts, rowid, fts_path) VALUES('delete', old.id, old.fts_path);
  INSERT INTO files_fts(rowid, fts_path) VALUES (new.id, new.fts_path);
END;