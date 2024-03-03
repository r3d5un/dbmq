CREATE QUEUE dbo.RequestQueue
WITH 
    STATUS=ON
    --, RETENTION=ON;
GO

CREATE QUEUE dbo.ResponseQueue
WITH 
    STATUS=ON
    --, RETENTION=ON;
GO
