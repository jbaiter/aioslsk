-- Older scan writes used '' for both pending and completed-empty attributes.
-- Treat those ambiguous entries as pending once. New writes use NULL for pending
-- and '' for completed-empty, so unchanged non-audio files can also be reused.
UPDATE files SET attributes = NULL WHERE attributes = '';
