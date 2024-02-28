INSERT INTO messages (data)
VALUES (
    '{"message": "Hello, world!"}'
)
RETURNING id, data, created_at;

NOTIFY message_channel, 'New message';