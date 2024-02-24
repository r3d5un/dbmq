INSERT INTO messages (data)
VALUES (
    '{"message": "Hello, world!"}'
);

NOTIFY dbmq_channel, 'New message available';
