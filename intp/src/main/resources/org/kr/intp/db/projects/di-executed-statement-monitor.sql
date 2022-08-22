--$$GEN_SCHEMA$$#ESM_DI_ESM_TRACKED_STATEMENTS_SEQ#SEQUENCE#1
create sequence "$$GEN_SCHEMA$$"."$$NAME$$" increment by 1 start with 1 minvalue 1 maxvalue 4611686018427387903 no cycle

--$$SCHEMA$$#DI_ESM_TRACKED_STATEMENTS#TABLE#0
CREATE COLUMN TABLE "$$SCHEMA$$"."$$NAME$$" (
     "HOST" NVARCHAR(64),
	 "PORT" INTEGER CS_INT,
	 "CONNECTION_ID" INTEGER CS_INT,
	 "TRANSACTION_ID" INTEGER CS_INT,
	 "STATEMENT_ID" NVARCHAR(256),
	 "STATEMENT_HASH" VARCHAR(32),
	 "DB_USER" NVARCHAR(256),
	 "APP_USER" NVARCHAR(256),
	 "START_TIME" LONGDATE CS_LONGDATE,
	 "STATEMENT_START_TIME" LONGDATE CS_LONGDATE,
	 "DURATION_MICROSEC" BIGINT CS_FIXED,
	 "OBJECT_NAME" NVARCHAR(5000),
	 "OPERATION" NVARCHAR(5000),
	 "RECORDS" BIGINT CS_FIXED,
	 "STATEMENT_STRING" NCLOB MEMORY THRESHOLD 1000,
	 "PARAMETERS" NVARCHAR(5000),
	 "MEMORY_SIZE" BIGINT CS_FIXED,
	 "REUSED_MEMORY_SIZE" BIGINT CS_FIXED,
	 "CPU_TIME" BIGINT CS_FIXED,
	 "APPLICATION_SOURCE" NVARCHAR(256),
	 "APPLICATION_NAME" NVARCHAR(256),
	 "PROC_NAME" NVARCHAR(255),
	 "TS_SECONDS" SECONDDATE CS_SECONDDATE,
	 "CPU_UTIL" SMALLINT CS_INT,
	 "CPU_PERCENT_SNAPSHOT" SMALLINT CS_INT,
	 "CPU_PERCENT_TOTAL" SMALLINT CS_INT
)

--$$SCHEMA$$#DI_ESM_ACCESSED_OBJECT#TABLE#0
CREATE COLUMN TABLE "$$SCHEMA$$"."$$NAME$$" (
     "STATEMENT_ID" NVARCHAR(256),
	 "SCHEMA_NAME" NVARCHAR(256),
	 "OBJECT_NAME" NVARCHAR(256)
)
--$$SCHEMA$$#DI_ESM_HELPER#TABLE#0
CREATE COLUMN TABLE "$$SCHEMA$$"."$$NAME$$" (
    "VAL" SMALLINT CS_INT,
	 PRIMARY KEY ("VAL")
)

--$$GEN_SCHEMA$$#ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG#TABLE#0
CREATE ROW TABLE "$$GEN_SCHEMA$$"."$$NAME$$" (
     "TYPE" CHAR(1) CS_FIXEDSTRING,
	 "UPD_TIMESTAMP" LONGDATE CS_LONGDATE,
	 "UUID" NVARCHAR(64) CS_STRING,
	 "STATUS" INT CS_INT,
	 "COMMENT" NVARCHAR(1023) CS_STRING,
	 "JOB_NAME" NVARCHAR(63) CS_STRING,
	 "FK_TRIGGERED" LONGDATE CS_LONGDATE,
	 "TRIGGERED" LONGDATE CS_LONGDATE,
	 "STARTED" LONGDATE CS_LONGDATE,
	 "FINISHED" LONGDATE CS_LONGDATE,
	 "HOST" NVARCHAR(64) CS_STRING
)

--$$GEN_SCHEMA$$#ESM_DI_ESM_TRACKED_STATEMENTS_TRIGGER_LOG#TABLE#0
CREATE ROW TABLE "$$GEN_SCHEMA$$"."$$NAME$$" (
     "ID" BIGINT CS_FIXED,
	 "UPD_TIMESTAMP" LONGDATE CS_LONGDATE,
	 "ACTION" CHAR(1) CS_FIXEDSTRING,
	 "HOST" NVARCHAR(64) CS_STRING,
	 PRIMARY KEY ( "ID" )
)

--$$SCHEMA$$#DI_ESM_INITIAL#PROCEDURE#2
CREATE PROCEDURE "$$SCHEMA$$"."$$NAME$$"(
	IN job_id NVARCHAR(64),
	IN job_ts TIMESTAMP,
	IN host NVARCHAR(64)
)
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
BEGIN

	/* Step 1: Persist all existing entries FROM the M_EXPENSIVE_STATEMENTS view to a table */
	DELETE FROM "$$SCHEMA$$"."DI_ESM_TRACKED_STATEMENTS";

	extract_FROM_expensive_statement_trace = SELECT
		HOST, PORT, CONNECTION_ID, TRANSACTION_ID, STATEMENT_ID, STATEMENT_HASH,
		DB_USER, APP_USER, START_TIME, STATEMENT_START_TIME, DURATION_MICROSEC, OBJECT_NAME, OPERATION,
		RECORDS, STATEMENT_STRING, PARAMETERS, MEMORY_SIZE,REUSED_MEMORY_SIZE, CPU_TIME, APPLICATION_SOURCE, APPLICATION_NAME,
		CASE WHEN OPERATION = 'CALL' THEN LTRIM (SUBSTR_REGEXPR('\."([A-Za-z]|_)*' IN TO_NCLOB("STATEMENT_STRING")),'."') ELSE NULL END PROC_NAME,
		TO_SECONDDATE(START_TIME) AS TS_SECONDS
		FROM "SYS"."M_EXPENSIVE_STATEMENTS"
		WHERE (HOST = :host OR :host = '_NO_LIM_') AND
		(
			OPERATION = 'CALL'
			OR OPERATION = 'SELECT'
		);

	/* Retrieves the CPU utilizatiON of the next snapshot time based ON the statement start time. */
	statements_with_cpu_utilizatiON = SELECT HOST, PORT, CONNECTION_ID, TRANSACTION_ID, STATEMENT_ID, STATEMENT_HASH,
		DB_USER, APP_USER, START_TIME, STATEMENT_START_TIME, DURATION_MICROSEC, OBJECT_NAME, OPERATION,
		RECORDS, STATEMENT_STRING, PARAMETERS, MEMORY_SIZE,REUSED_MEMORY_SIZE, CPU_TIME, APPLICATION_SOURCE, APPLICATION_NAME,
		PROC_NAME, l.TS_SECONDS, PROCESS_CPU,
		SUM(CPU_TIME) OVER (PARTITION BY l.TS_SECONDS) AS CPU_TIME_SUM
		FROM :extract_FROM_expensive_statement_trace l
		LEFT JOIN "$$SCHEMA$$"."DI_ISM_STATISTICS" r
		ON l.TS_SECONDS < r.TS_SECONDS AND ADD_SECONDS(l.TS_SECONDS,5) >= r.TS_SECONDS;

	statements_with_all_cpu_informatiON = SELECT HOST, PORT, CONNECTION_ID, TRANSACTION_ID, STATEMENT_ID, STATEMENT_HASH,
		DB_USER, APP_USER, START_TIME, STATEMENT_START_TIME, DURATION_MICROSEC, OBJECT_NAME, OPERATION,
		RECORDS, STATEMENT_STRING, PARAMETERS, MEMORY_SIZE,REUSED_MEMORY_SIZE, CPU_TIME, APPLICATION_SOURCE, APPLICATION_NAME,
		PROC_NAME, TS_SECONDS, PROCESS_CPU,
		CASE WHEN CPU_TIME_SUM != 0 THEN CPU_TIME/CPU_TIME_SUM*100 ELSE 0 END AS CPU_PERCENT_SNAPSHOT,
		CASE WHEN CPU_TIME_SUM != 0 THEN CPU_TIME/CPU_TIME_SUM*PROCESS_CPU ELSE 0 END AS CPU_PERCENT_TOTAL
		FROM :statements_with_cpu_utilizatiON;

	INSERT INTO "$$SCHEMA$$"."DI_ESM_TRACKED_STATEMENTS" SELECT * FROM :statements_with_all_cpu_informatiON;

	/* Step 2: Extract the accessed objects AND store them in a table. */
	DELETE FROM "$$SCHEMA$$"."DI_ESM_ACCESSED_OBJECT";

	stats = SELECT STATEMENT_ID, STATEMENT_STRING, OBJECT_NAME, OCCURRENCES_REGEXPR(',' IN OBJECT_NAME)+1 AS NUMBER_OF_OBJECTS
	FROM "$$SCHEMA$$"."DI_ESM_TRACKED_STATEMENTS";

	intermediate_result = SELECT *
	FROM :stats l
	LEFT JOIN "$$SCHEMA$$"."DI_ESM_HELPER" r
	ON l.NUMBER_OF_OBJECTS >= r.VAL;

	objects = SELECT STATEMENT_ID, SUBSTR_REGEXPR('(\w*\.?\w*)(,)?' IN OBJECT_NAME OCCURRENCE VAL GROUP 1) AS OBJECT_NAME
	FROM :intermediate_result;

	schema_obj_sep = SELECT STATEMENT_ID,
	SUBSTR_REGEXPR('(\w*)(\.)(\w*)' IN OBJECT_NAME GROUP 1) AS SCHEMA_NAME,
	SUBSTR_REGEXPR('(\w*)(\.)(\w*)' IN OBJECT_NAME GROUP 3) AS OBJECT_NAME
	FROM :objects;


	INSERT INTO "$$SCHEMA$$"."DI_ESM_ACCESSED_OBJECT" SELECT STATEMENT_ID, SCHEMA_NAME, OBJECT_NAME
	FROM :schema_obj_sep
	WHERE SCHEMA_NAME != 'SYS';
END

--$$SCHEMA$$#DI_ESM_DELTA#PROCEDURE#0
CREATE PROCEDURE "$$SCHEMA$$"."$$NAME$$"(
	IN job_id NVARCHAR(64),
	IN job_ts TIMESTAMP,
	IN host NVARCHAR(64)
)
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
BEGIN

	DECLARE last_fetch_timestamp TIMESTAMP;
	host_data_exists = SELECT TOP 1 START_TIME
		FROM "$$SCHEMA$$"."DI_ESM_TRACKED_STATEMENTS"
		WHERE HOST = :host OR :host = '_NO_LIM_'
		ORDER BY START_TIME DESC;

	IF IS_EMPTY(:host_data_exists) THEN
		RETURN;
	ELSE
		/* Step 1: Perform updates */
		SELECT TOP 1 START_TIME INTO last_fetch_timestamp
		FROM "$$SCHEMA$$"."DI_ESM_TRACKED_STATEMENTS"
		WHERE HOST = :host OR :host = '_NO_LIM_'
		ORDER BY START_TIME DESC;

		extract_FROM_expensive_statement_trace = SELECT
			HOST, PORT, CONNECTION_ID, TRANSACTION_ID, STATEMENT_ID, STATEMENT_HASH,
			DB_USER, APP_USER, START_TIME, STATEMENT_START_TIME, DURATION_MICROSEC, OBJECT_NAME, OPERATION,
			RECORDS, STATEMENT_STRING, PARAMETERS, MEMORY_SIZE,REUSED_MEMORY_SIZE, CPU_TIME, APPLICATION_SOURCE, APPLICATION_NAME,
			CASE WHEN OPERATION = 'CALL' THEN LTRIM (SUBSTR_REGEXPR('\."([A-Za-z]|_)*' IN TO_NCLOB("STATEMENT_STRING")),'."') ELSE NULL END PROC_NAME,
			TO_SECONDDATE(START_TIME) AS TS_SECONDS
			FROM "SYS"."M_EXPENSIVE_STATEMENTS"
			WHERE (HOST = :host OR :host = '_NO_LIM_') AND START_TIME > :last_fetch_timestamp AND
			(
				OPERATION = 'CALL'
				OR OPERATION = 'SELECT'
			);

		/* Retrieves the CPU utilizatiON of the next snapshot time based ON the statement start time. */
		statements_with_cpu_utilizatiON = SELECT HOST, PORT, CONNECTION_ID, TRANSACTION_ID, STATEMENT_ID, STATEMENT_HASH,
			DB_USER, APP_USER, START_TIME, STATEMENT_START_TIME, DURATION_MICROSEC, OBJECT_NAME, OPERATION,
			RECORDS, STATEMENT_STRING, PARAMETERS, MEMORY_SIZE,REUSED_MEMORY_SIZE, CPU_TIME, APPLICATION_SOURCE, APPLICATION_NAME,
			PROC_NAME, l.TS_SECONDS, PROCESS_CPU,
			SUM(CPU_TIME) OVER (PARTITION BY l.TS_SECONDS) AS CPU_TIME_SUM
			FROM :extract_FROM_expensive_statement_trace l
			LEFT JOIN "$$SCHEMA$$"."DI_ISM_STATISTICS" r
			ON l.TS_SECONDS < r.TS_SECONDS AND ADD_SECONDS(l.TS_SECONDS,5) >= r.TS_SECONDS;

		statements_with_all_cpu_informatiON = SELECT HOST, PORT, CONNECTION_ID, TRANSACTION_ID, STATEMENT_ID, STATEMENT_HASH,
			DB_USER, APP_USER, START_TIME, STATEMENT_START_TIME, DURATION_MICROSEC, OBJECT_NAME, OPERATION,
			RECORDS, STATEMENT_STRING, PARAMETERS, MEMORY_SIZE,REUSED_MEMORY_SIZE, CPU_TIME, APPLICATION_SOURCE, APPLICATION_NAME,
			PROC_NAME, TS_SECONDS, PROCESS_CPU,
			CASE WHEN CPU_TIME_SUM != 0 THEN CPU_TIME/CPU_TIME_SUM*100 ELSE 0 END AS CPU_PERCENT_SNAPSHOT,
			CASE WHEN CPU_TIME_SUM != 0 THEN CPU_TIME/CPU_TIME_SUM*PROCESS_CPU ELSE 0 END AS CPU_PERCENT_TOTAL
			FROM :statements_with_cpu_utilizatiON;

		INSERT INTO "$$SCHEMA$$"."DI_ESM_TRACKED_STATEMENTS" SELECT * FROM :statements_with_all_cpu_informatiON;

		/* Step 2: Extract the accessed objects AND store them in a table. */
		stats = SELECT STATEMENT_ID, STATEMENT_STRING, OBJECT_NAME, OCCURRENCES_REGEXPR(',' IN OBJECT_NAME)+1 AS NUMBER_OF_OBJECTS
		FROM "$$SCHEMA$$"."DI_ESM_TRACKED_STATEMENTS"
		WHERE START_TIME > :last_fetch_timestamp;

		intermediate_result = SELECT *
		FROM :stats l
		LEFT JOIN "$$SCHEMA$$"."DI_ESM_HELPER" r
		ON l.NUMBER_OF_OBJECTS >= r.VAL;

		objects = SELECT STATEMENT_ID, SUBSTR_REGEXPR('(\w*\.?\w*)(,)?' IN OBJECT_NAME OCCURRENCE VAL GROUP 1) AS OBJECT_NAME
		FROM :intermediate_result;

		schema_obj_sep = SELECT STATEMENT_ID,
		SUBSTR_REGEXPR('(\w*)(\.)(\w*)' IN OBJECT_NAME GROUP 1) AS SCHEMA_NAME,
		SUBSTR_REGEXPR('(\w*)(\.)(\w*)' IN OBJECT_NAME GROUP 3) AS OBJECT_NAME
		FROM :objects;


		INSERT INTO "$$SCHEMA$$"."DI_ESM_ACCESSED_OBJECT" SELECT STATEMENT_ID, SCHEMA_NAME, OBJECT_NAME
		FROM :schema_obj_sep
		WHERE SCHEMA_NAME != 'SYS';
	END IF;
END

--$$GEN_SCHEMA$$#ESM_DI_ESM_TRACKED_STATEMENTS_GET_INITIAL_KEYS#PROCEDURE#0
CREATE PROCEDURE "$$GEN_SCHEMA$$"."$$NAME$$"(
  IN job_id NVARCHAR(64),
  IN job_ts TIMESTAMP
)
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS
BEGIN
	SELECT DISTINCT HOST FROM M_HOST_INFORMATION;
END;

--$$GEN_SCHEMA$$#ESM_DI_ESM_TRACKED_STATEMENTS_GET_DELTA_KEYS#PROCEDURE#0
CREATE PROCEDURE "$$GEN_SCHEMA$$"."$$NAME$$"(
  IN job_id NVARCHAR(64),
  IN job_ts TIMESTAMP
)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER AS
BEGIN
	SELECT '_NO_LIM_' FROM DUMMY;
END

--$$GEN_SCHEMA$$#ESM_DI_ESM_TRACKED_STATEMENTS_RESTART_ERRORS#PROCEDURE#0
CREATE PROCEDURE "$$GEN_SCHEMA$$"."$$NAME$$"(
)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER AS
BEGIN
	uuids =
		SELECT UUID
		FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG"
		WHERE STATUS = (SELECT ID FROM "$$SCHEMA$$"."RT_STATUS_DESC" WHERE NAME = 'E')
		FOR UPDATE;

	keys = SELECT DISTINCT HOST AS HOST
		FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG"
		WHERE STATUS = (SELECT ID FROM "$$SCHEMA$$"."RT_STATUS_DESC" WHERE NAME = 'B')
			AND UUID IN (SELECT UUID FROM :UUIDS);

	INSERT INTO "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_TRIGGER_LOG"
	SELECT
		"$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_SEQ".NEXTVAL,
		CURRENT_UTCTIMESTAMP,
		'r',
		HOST
	FROM :keys;

	DELETE FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" WHERE UUID IN (SELECT UUID FROM :uuids);
END

--$$GEN_SCHEMA$$#ESM_CLEANUP_TRIGGER_LOG#PROCEDURE#0
CREATE PROCEDURE "$$GEN_SCHEMA$$"."$$NAME$$"()
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS
BEGIN
	DELETE FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_TRIGGER_LOG";
END

--$$GEN_SCHEMA$$#ESM_ARCHIVE_RUN_LOG#PROCEDURE#0
CREATE PROCEDURE "$$GEN_SCHEMA$$"."$$NAME$$"
	SQL SECURITY INVOKER AS
BEGIN
	uuids = SELECT UUID FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" WHERE STATUS = 50;

	DATA = SELECT T.JOB_NAME, T.TYPE, T.UPD_TIMESTAMP, T.UUID, T.TRIGGERED, S.STARTED, F.FINISHED,0 AS SLA_MILLIS
		FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" T
			INNER JOIN "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" S  ON  T.UUID = S.UUID AND T.STATUS = 0 AND S.STATUS = 20
			INNER JOIN "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" F  ON T.UUID = F.UUID AND F.STATUS = 50;

	INSERT INTO "$$SCHEMA$$"."RT_RUN_LOG_HISTORY"  SELECT * FROM :DATA;

	DELETE FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" WHERE UUID IN (SELECT UUID FROM :uuids);
END

--$$INTP$$#ESM#STATEMENT#3
DO
BEGIN
    DECLARE v_index INTEGER := 0;

    UPSERT "$$SCHEMA$$"."RT_PROJECT_DEFINITIONS" VALUES ('$$NAME$$', 0, 'EVENT', 'Executed Statement Monitor', 'Monitors AND stores all executed statements against the database.', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, '$$SCHEMA$$', '$$SCHEMA$$', 0, 5, 3, 0) WHERE PROJECT_ID = '$$NAME$$' AND VERSION = 0;
    UPSERT "$$SCHEMA$$"."RT_PROJECT_KEYS" VALUES ('$$NAME$$', 1, 1, 0, 'HOST', 'NVARCHAR(64)', 'X', '', 10, 'X', '') WHERE PROJECT_ID = '$$NAME$$' AND VERSION = 0;
    UPSERT "$$SCHEMA$$"."RT_PROJECT_PROCEDURES" VALUES ('$$NAME$$', 1, 'I', 'DI_ESM_INITIAL',0,'SP','HAVA_DEV') WHERE PROJECT_ID = '$$NAME$$' AND VERSION = 0 AND TYPE ='I';
    UPSERT "$$SCHEMA$$"."RT_PROJECT_PROCEDURES" VALUES ('$$NAME$$', 1, 'D', 'DI_ESM_DELTA',0,'SP','HAVA_DEV') WHERE PROJECT_ID = '$$NAME$$' AND VERSION = 0 AND TYPE ='D';
    UPSERT "$$SCHEMA$$"."RT_ACTIVE_PROJECTS" VALUES ('$$NAME$$', 0, 0, '',null,'Stopped', true) WHERE PROJECT_ID = '$$NAME$$' AND VERSION = 0;
    UPSERT "$$SCHEMA$$"."RT_LIFECYCLE_JOBS" VALUES ('$$NAME$$', 'ARCHIVE RUN_LOG', '"$$GEN_SCHEMA$$"."ESM_ARCHIVE_RUN_LOG"', 0, null, null, 1, 'Y') WHERE PROJECT_ID = '$$NAME$$';
    UPSERT "$$SCHEMA$$"."RT_PROJECT_TABLES" VALUES ('$$NAME$$', 1, 'DI_ESM_TRACKED_STATEMENTS', 0) WHERE PROJECT_ID = '$$NAME$$' AND VERSION = 0;
    UPSERT "$$SCHEMA$$"."RT_PARAMETERS" VALUES ('$$NAME$$', 0, CURRENT_TIMESTAMP,
    '{"initial":{"calendar":{"firstdayofcalendaryear":null,"lastdayofcalendaryear":null,"firstdayoffiscalyear":null,"lastdayoffiscalyear":null,"firstdayofcalendarmONth":null,"lastdayofcalendarmONth":null,"firstdayofaccountingperiod":null,"lastdayofaccountingperiod":null,"firstdayofpayrollperiod":null,"lastdayofpayrollperiod":null,"firstdayofbiweeklyperiod":null,"lastdayofbiweeklyperiod":null},"executors":2,"conn_type":"pool","period":0,"fullload":false,"flrestrictiONs":"","scheduling":"PERIOD","periodchoice":"SECONDS"},"delta":{"calendar":{"firstdayofcalendaryear":null,"lastdayofcalendaryear":null,"firstdayoffiscalyear":null,"lastdayoffiscalyear":null,"firstdayofcalendarmONth":null,"lastdayofcalendarmONth":null,"firstdayofaccountingperiod":null,"lastdayofaccountingperiod":null,"firstdayofpayrollperiod":null,"lastdayofpayrollperiod":null,"firstdayofbiweeklyperiod":null,"lastdayofbiweeklyperiod":null},"executors":2,"conn_type":"pool","period":60,"scheduling":"PERIOD","periodchoice":"SECONDS"},"history":false,"sla":200}') WHERE PROJECT_ID = '$$NAME$$' AND VERSION = 0;

    FOR v_index IN 0..100 DO
        UPSERT "$$SCHEMA$$"."DI_ESM_HELPER" VALUES (:v_index) WHERE VAL = :v_index;
    END FOR;
END

--$$GEN_SCHEMA$$#ESM_RUNTIME_INFO#VIEW#0
CREATE VIEW "$$GEN_SCHEMA$$"."ESM_RUNTIME_INFO" (
    "TYPE",
	 "STATUS",
	 "UUID",
	 "FK_TRIGGERED",
	 "TRIGGERED",
	 "STARTED",
	 "P-STARTED",
	 "P-FINISHED",
	 "WAIT_TIME",
	 "PROC_TIME",
	 "ALL_TIME",
	 "HOST" ) AS select
	 t.TYPE,
	 CASE WHEN e.UUID IS NOT NULL
THEN 'ERROR' WHEN f.UUID IS NOT NULL
THEN 'DONE' WHEN p.UUID IS NOT NULL
THEN 'RUN' WHEN s.UUID IS NOT NULL
THEN 'STARTED'
ELSE 'WAIT'
END AS STATE,
	 t.UUID,
	 TO_VARCHAR(t.FK_TRIGGERED,
	 'DD-MM-YYYY HH24:MI:SS' ) as FK_TRIGGERED,
	 TO_VARCHAR(t.TRIGGERED,
	 'DD-MM-YYYY HH24:MI:SS' ) as TRIGGERED,
	 TO_VARCHAR(s.STARTED ,
	 'DD-MM-YYYY HH24:MI:SS' ) as STARTED,
	 TO_VARCHAR(p.STARTED,
	 'HH24:MI:SS.FF3' ) as "P-STARTED",
	 TO_VARCHAR(p.FINISHED,
	 'HH24:MI:SS.FF3' ) as "P-FINISHED",
	 NANO100_BETWEEN(t.FK_TRIGGERED,
	 s.STARTED) / 10000000.0 as WAIT_TIME,
	 NANO100_BETWEEN(p.STARTED,
	 p.FINISHED) / 10000000.0 as PROC_TIME,
	 NANO100_BETWEEN(t.FK_TRIGGERED,
	 f.FINISHED) / 10000000.0 as ALL_TIME,
	 p.HOST
FROM "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" t
LEFT JOIN "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" s ON t.UUID = s.UUID
AND s.STATUS = (select
	 id
	FROM "$$SCHEMA$$"."RT_STATUS_DESC"
	WHERE NAME = 'S')
LEFT JOIN "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" p ON t.UUID = p.UUID
AND p.STATUS = (select
	 id
	FROM "$$SCHEMA$$"."RT_STATUS_DESC"
	WHERE NAME = 'P')
LEFT JOIN "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" f ON t.UUID = f.UUID
AND f.STATUS = (select
	 id
	FROM "$$SCHEMA$$"."RT_STATUS_DESC"
	WHERE NAME = 'F')
LEFT JOIN "$$GEN_SCHEMA$$"."ESM_DI_ESM_TRACKED_STATEMENTS_RUN_LOG" e ON t.UUID = e.UUID
AND e.STATUS = (select
	 id
	FROM "$$SCHEMA$$"."RT_STATUS_DESC"
	WHERE NAME = 'E')
WHERE t.STATUS = (select
	 id
	FROM "$$SCHEMA$$"."RT_STATUS_DESC"
	WHERE NAME = 'T')
ORDER BY t.TRIGGERED DESC WITH READ ONLY

--$$GEN_SCHEMA$$#ESM_LIFECYCLE_LOG#VIEW#0
CREATE VIEW "$$GEN_SCHEMA$$"."ESM_LIFECYCLE_LOG" ( "TRIGGERED",
	 "STATUS",
	 "NAME",
	 "WAIT_TIME",
	 "PROC_TIME",
	 "ALL_TIME",
	 "COMMENT" ) AS select
	 t.UPDATE_TS as TRIGGERED,
	 CASE WHEN e.UUID IS NOT NULL
THEN 'ERROR' WHEN f.UUID IS NOT NULL
THEN 'FINISHED' WHEN s.UUID IS NOT NULL
THEN 'STARTED'
ELSE 'TRIGGERED'
END AS STATUS,
	 t.NAME,
	 nano100_between(t.UPDATE_TS,
	 s.UPDATE_TS) / 10000000.0 as WAIT_TIME,
	 nano100_between(s.UPDATE_TS,
	 f.UPDATE_TS) / 10000000.0 as PROC_TIME,
	 nano100_between(t.UPDATE_TS,
	 f.UPDATE_TS) / 10000000.0 as ALL_TIME,
	 e.comment
FROM "$$SCHEMA$$"."RT_LIFECYCLE_LOG" t
LEFT JOIN "$$SCHEMA$$"."RT_LIFECYCLE_LOG" s ON t.UUID = s.UUID
AND t.STATUS = 0
AND s.STATUS = 20
LEFT JOIN "$$SCHEMA$$"."RT_LIFECYCLE_LOG" f ON t.UUID = f.UUID
AND f.STATUS = 50
LEFT JOIN "$$SCHEMA$$"."RT_LIFECYCLE_LOG" e ON t.UUID = e.UUID
AND e.STATUS = 100
WHERE t.PROJECT_ID = 'ESM'
ORDER BY 1 DESC WITH READ ONLY