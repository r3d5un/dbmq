DECLARE @conversation_handle UNIQUEIDENTIFIER;
DECLARE @message_type_name NVARCHAR(256);
DECLARE @message_body NVARCHAR(MAX);

BEGIN TRANSACTION;

RECEIVE TOP(1)
    @conversation_handle = conversation_handle,
    @message_type_name = message_type_name,
    @message_body = message_body
FROM dbo.ResponseQueue;

PRINT 'Conversation handle: ' + CAST(@conversation_handle as NVARCHAR(MAX))
PRINT 'Message type: ' + @message_type_name
PRINT 'Message body: ' + CAST(@message_body as NVARCHAR(MAX))

IF (@message_type_name = 'ResponseMessage')
    BEGIN
        END CONVERSATION @conversation_handle
    END

COMMIT TRANSACTION;
