SELECT message_body, conversation_handle, message_type_name
FROM dbo.RequestQueue WITH (NOLOCK);
