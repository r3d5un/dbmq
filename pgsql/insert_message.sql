INSERT INTO messages (data)
VALUES (
    '{"message": "Hello, world!"}'
);

NOTIFY message_channel, 'New message';