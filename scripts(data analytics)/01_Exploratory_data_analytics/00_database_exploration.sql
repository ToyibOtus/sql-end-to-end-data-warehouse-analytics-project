/*
===================================================================
Database & Objects Exploration
===================================================================
Script Purpose:
	This script explores all relevant objects in [SalesDatabase].
	It aims to identify the number of relevant objects, and their
	respective fields.
===================================================================
*/
USE SalesDatabase;
GO

-- Explore gold objects in Salesdatabase
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold';

-- Explore columns in each gold object
SELECT 
	TABLE_CATALOG, 
	TABLE_SCHEMA, 
	TABLE_NAME, 
	COLUMN_NAME, 
	ORDINAL_POSITION 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold'
ORDER BY TABLE_NAME, ORDINAL_POSITION;
