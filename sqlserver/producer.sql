DECLARE @message NVARCHAR(MAX);
DECLARE @conversation_handle UNIQUEIDENTIFIER;

BEGIN TRANSACTION

-- Start a new conversation
BEGIN DIALOG @conversation_handle
FROM SERVICE [RequestService]
TO SERVICE 'ResponseService'
ON CONTRACT [ProcessingContract]
WITH ENCRYPTION = OFF;

-- Message content
SET @message = N'Hello, World!';

-- Send the message
SEND ON CONVERSATION @conversation_handle
MESSAGE TYPE [RequestMessage] (@message);

SELECT @message AS SentMessage, @conversation_handle AS id;

COMMIT TRANSACTION;