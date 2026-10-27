-- V0002: Add transfers table to track file uploads/downloads.

-- Tracks transfers, corresponding Python class is transfer.model.Transfer
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- See transfer.state.TransferState enum
    state INTEGER NOT NULL CHECK(state BETWEEN -1 AND 10),
    -- 0 = upload, 1 = download
    direction INTEGER NOT NULL CHECK(direction BETWEEN 0 AND 1),
    username TEXT NOT NULL,
    remote_path TEXT NOT NULL,
    local_path TEXT,
    remotely_queued INTEGER NOT NULL DEFAULT 0 CHECK(remotely_queued BETWEEN 0 AND 1),
    place_in_queue INTEGER,
    -- 0 = cancelled, 1 = complete, 2 = queued, 3 = file not shared,
    -- 4 = file read error, 5 = custom
    fail_reason INTEGER CHECK(fail_reason BETWEEN 0 AND 5),
    fail_reason_text TEXT,
    -- 0 = requested, 1 = blocked, 2 = file not shared, 3 = custom
    abort_reason INTEGER CHECK(abort_reason BETWEEN 0 AND 3),
    abort_reason_text TEXT,
    file_size INTEGER,
    bytes_transferred INTEGER NOT NULL DEFAULT 0,
    queue_attempts INTEGER NOT NULL DEFAULT 0,
    last_queue_attempt TIMESTAMP,
    upload_request_attempts INTEGER NOT NULL DEFAULT 0,
    last_upload_request_attempt TIMESTAMP,
    start_time TIMESTAMP,
    complete_time TIMESTAMP,
    UNIQUE (username, remote_path, direction)
);

-- No indices for now, since this table is currently only used for inserts/updates/deletes and
-- bulk retrieval at application startup.
