CREATE PROCEDURE UpdateWatermarkTable
	@lastload varchar(200)
AS
BEGIN
	
	 BEGIN TRANSACTION;
	 
	 UPDATE [dbo].[water_table]
	 SET last_load = @lastload
	COMMIT TRANSACTION;
	 END;