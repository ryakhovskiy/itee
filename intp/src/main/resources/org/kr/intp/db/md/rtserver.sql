CREATE ROW TABLE $$SCHEMA$$."RT_SERVER"(
	"ID" INT CS_INT,
	"INSTANCE_ID" VARCHAR(3) CS_STRING,
    "NAME" VARCHAR(100) CS_STRING,
    "TYPE" VARCHAR CS_STRING,
    "INTP_HOST" VARCHAR(50) CS_STRING,
    "PORT" INT CS_INT,
    "STATUS" TINYINT CS_INT,
    "HANA_HOST" VARCHAR(50) CS_STRING,
    "HANA_INSTANCE" VARCHAR(2) CS_STRING,
    "INTP_LICENSE_KEY" VARCHAR(4000) CS_STRING,
    "HANA_USER" NVARCHAR(100) CS_STRING,
    "HANA_PASSWORD" NVARCHAR(100) CS_STRING,
    "LDETAILS" NVARCHAR(512) CS_STRING,
    "INTP_SIZE_MB" INT CS_INT,
    "GENERATED_OBJECTS_SCHEMA" NVARCHAR(128) CS_STRING,
    "COMMUNICATION_METHOD" INT CS_INT,
    "FI_CALENDAR_SCHEMA" NVARCHAR(128) CS_STRING,
    "FI_CALENDAR_TABLE" NVARCHAR(128) CS_STRING,
    "POWER_MON" NVARCHAR(400) CS_STRING,
PRIMARY KEY ( "ID","INSTANCE_ID" ))