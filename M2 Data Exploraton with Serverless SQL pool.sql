
BEGIN --	Demo Serverless SQL Pool as an Azure SQL Server Instance
    SELECT @@VERSION
    END
GO
BEGIN --    Basic query of Data Lake csv file
    SELECT
		TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://dcadlsgen2.blob.core.windows.net/data/ehr/bronz/TX/Patients.csv',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0'
            ) AS [rows]

    END
GO
BEGIN --    Query of Data Lake csv file - Adding header
    SELECT
		TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://dcadlsgen2.blob.core.windows.net/data/ehr/bronz/TX/Patients.csv',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            HEADER_ROW=TRUE
            ) AS [rows]

    END
GO
BEGIN --    Query of Data Lake parquet files (multiple file)
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://dcadlsgen2.blob.core.windows.net/data/ehr/silver/Appointments/**',
            FORMAT = 'PARQUET'
            ) AS [rows]

    END
GO
BEGIN --    Query of Data Lake parquet files using Wildcard with filtered partitions
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://dcadlsgen2.blob.core.windows.net/data/ehr/silver/Appointments/Year=*/Month=*/*.*',
            FORMAT = 'PARQUET'
            ) AS [rows]
        WHERE [rows].filepath(1)='2024' AND [rows].filepath(2) IN (1,2,3)

    END
GO
BEGIN --    Basic Exploration of data lake GA Doctors in JSON format file CROSS APPLY OPENJSON
    SELECT
        TOP 100 
        DoctorID,
        FirstName,
        LastName,
        Specialty,
        Source
    FROM
        OPENROWSET(
            BULK 'https://dcadlsgen2.blob.core.windows.net/data/ehr/silver/Doctors/**',
            FORMAT = 'CSV',
            FIELDTERMINATOR ='0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0b'
            ) WITH (doc NVARCHAR(MAX)) as rows
            CROSS APPLY
            OPENJSON(doc)
            WITH (
                DoctorID INT '$.DoctorID',
                FirstName VARCHAR(50) '$.FirstName',
                LastName VARCHAR(255) '$.LastName',
                Specialty VARCHAR(255) '$.Specialty',
                Source VARCHAR(255) '$.Source'
            ) AS jsonData
    END
GO
BEGIN --    Basic Exploration of data lake GA Doctors in JSON format file / 2nd Method using JSON_VALUE
    	SELECT
			TOP 100
			JSON_VALUE(doc, '$.DoctorID') AS DoctorID,
			JSON_VALUE(doc, '$.FirstName') AS FirstName,
            JSON_VALUE(doc, '$.LastName') AS LastName,
			JSON_VALUE(doc, '$.Specialty') AS Specialty,
            JSON_VALUE(doc, '$.Source') AS Source
		FROM
			OPENROWSET(
					BULK 'https://dcadlsgen2.blob.core.windows.net/data/ehr/silver/Doctors/**',
					FORMAT = 'CSV',
					FIELDTERMINATOR ='0x0b',
					FIELDQUOTE = '0x0b',
					ROWTERMINATOR = '0x0b'
					) WITH (doc NVARCHAR(MAX)) AS rows
    END
GO


BEGIN --    Creatin a Logical DW

    CREATE DATABASE EHRDemoDB
    COLLATE Latin1_General_100_BIN2_UTF8;

    --  master key to encrypt everything in the logical DW 

        CREATE MASTER KEY ENCRYPTION BY PASSWORD = '!QAZ@WSX3edc4rfv'; 

    --  Create a scope credential using your shared access signature

        CREATE DATABASE SCOPED CREDENTIAL DwScopeCredential
            WITH
                IDENTITY='SHARED ACCESS SIGNATURE',  
                SECRET = 'YOUR_OWN_SHARED_ACCESS_SIGNATURE';

    --	Create an external data source

        CREATE EXTERNAL DATA SOURCE DWDatasource
        WITH ( LOCATION = 'https://dcadlsgen2.dfs.core.windows.net/data/',
                    CREDENTIAL = DwScopeCredential);

    --	Define file format

        CREATE EXTERNAL FILE FORMAT ParquetFormat
            WITH ( FORMAT_TYPE = PARQUET);

    --  Creating external table

        CREATE EXTERNAL TABLE dbo.ET_Appointments
        (
            AppointmentID INT,
            PatientKey INT,
            DoctorKey INT,
            AppointmentDate DATETIME,
            Reason NVARCHAR(225)
        )
        WITH
        (
            DATA_SOURCE = DWDatasource,
            LOCATION = 'ehr/silver/Appointments/**',
            FILE_FORMAT = ParquetFormat
        )

        SELECT top 1000 * FROM dbo.ET_Appointments WHERE AppointmentID=75996 
    --  Create a View 

        CREATE VIEW vFluSeasonAppointments
        AS 
        SELECT
            TOP 100 *
        FROM
            OPENROWSET(
                BULK 'https://dcadlsgen2.blob.core.windows.net/data/ehr/silver/Appointments/Year=*/Month=*/*.*',
                FORMAT = 'PARQUET'
                ) AS [rows]
            WHERE [rows].filepath(1)='2024' AND [rows].filepath(2) IN (1,2,3)
        
        SELECT top 10 * FROM dbo.vFluSeasonAppointments

    --  Reset

        DROP EXTERNAL TABLE dbo.ET_Appointments; 
   
    END
GO


BEGIN
    CREATE PROCEDURE sp_SummerAppointments
    AS
    BEGIN
        -- drop existing table
        IF EXISTS (
                SELECT * FROM sys.external_tables
                WHERE name = 'ETAS_SummerAppointment'
            )
            DROP EXTERNAL TABLE ETAS_SummerAppointment
        
        -- create external table
        CREATE EXTERNAL TABLE ETAS_SummerAppointment
        WITH (
                LOCATION = 'ehr/silver/refined/summer/appointments',
                DATA_SOURCE = DWDatasource,
                FILE_FORMAT = ParquetFormat
            )
        AS
        SELECT             
            AppointmentID,
            PatientKey,
            DoctorKey,
            AppointmentDate,
            Reason
        FROM
            OPENROWSET(
                BULK 'ehr/silver/Appointments/Year=*/Month=*/*.*',
                DATA_SOURCE = 'DWDatasource',
                FORMAT = 'parquet'
            ) AS [rows]

            WHERE [rows].filepath(1)='2024' AND [rows].filepath(2) IN (6,7,6)
    END


    EXEC sp_SummerAppointments
    select top 10 * from ETAS_SummerAppointment

    END
GO

select top 10 * from Dimpatients





