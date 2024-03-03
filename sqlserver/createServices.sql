CREATE SERVICE [RequestService]
ON QUEUE dbo.RequestQueue ([ProcessingContract]);

CREATE SERVICE [ResponseService]
ON QUEUE dbo.ResponseQueue ([ProcessingContract]);
