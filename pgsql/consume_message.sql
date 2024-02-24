BEGIN;

SELECT id, data, created_at
FROM messages
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 1;

DELETE FROM messages
WHERE id IN (
    SELECT id
    FROM messages
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
);

COMMIT;

rollback;