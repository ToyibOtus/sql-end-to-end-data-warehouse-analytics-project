/*
====================================================================================
Database & Objects Exploration
====================================================================================
Script Purpose:
	This script explores all relevant objects in database [SalesDatabase].
====================================================================================
*/
USE SalesDatabase;
GO

SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold';


SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' ORDER BY TABLE_NAME, ORDINAL_POSITION;
