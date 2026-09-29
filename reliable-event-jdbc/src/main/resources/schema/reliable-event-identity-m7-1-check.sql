-- All counts must be zero before resuming writers or enabling cleanup.
SELECT COUNT(*) AS outbox_without_matching_identity
FROM reliable_event_outbox o
LEFT JOIN reliable_event_identity i ON i.id = o.id
    AND i.event_type = o.event_type AND i.event_key = o.event_key
WHERE i.id IS NULL;

SELECT COUNT(*) AS identity_key_mismatch
FROM reliable_event_identity i
JOIN reliable_event_outbox o ON o.event_type = i.event_type AND o.event_key = i.event_key
WHERE i.id <> o.id;

-- During initial migration, before cleanup, this must also be zero.
SELECT COUNT(*) AS identity_without_outbox_before_cleanup
FROM reliable_event_identity i
LEFT JOIN reliable_event_outbox o ON o.id = i.id
WHERE o.id IS NULL;

SELECT (SELECT COALESCE(MAX(id), 0) FROM reliable_event_outbox) AS max_outbox_id,
       (SELECT COALESCE(MAX(id), 0) FROM reliable_event_identity) AS max_identity_id,
       (SELECT AUTO_INCREMENT FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'reliable_event_identity')
           AS next_identity_id;
-- next_identity_id must exceed max_outbox_id before writers resume.

-- Compare column types and the unique (event_type, event_key) key with the shipped schema.
SHOW CREATE TABLE reliable_event_identity;
SHOW CREATE TABLE reliable_event_outbox;
