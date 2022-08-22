--$$SCHEMA$$#SET_RUNNING#PROCEDURE#1
CREATE PROCEDURE $$SCHEMA$$.SET_RUNNING(in instance_id nvarchar(3))
 LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS
	status integer;
begin
	update $$SCHEMA$$.RT_SERVER set status = 1 where instance_id = :instance_id;
end

--$$SCHEMA$$#ERE_STATUS#PROCEDURE#1
create procedure $$SCHEMA$$.ERE_STATUS(in project_id nvarchar(64))
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
as
begin
	select
		p.name as ERE_NAME,
		case when ifnull(i.MODE, 0) = 0 then 'Real-Time' else 'In-Time' end as MODE,
		case
			when ifnull(i.MODE, 0) = 0 then
				(select avg(avg_execution_time * 1000) as AVG_EXEC_TIME_MS
				from m_sql_plan_cache
				where statement_string like '%'||p.rt_object||'%'
					and statement_string not like '%m_sql_plan_cache%')
			else
				(select avg(avg_execution_time * 1000) as AVG_EXEC_TIME_MS
				from m_sql_plan_cache
				where statement_string like '%'||p.it_object||'%'
					and statement_string not like '%m_sql_plan_cache%')
			end as AVG_EXEC_TIME,
			case
			when ifnull(i.MODE, 0) = 0 then
				(select max(max_execution_time * 1000) as MAX_EXEC_TIME_MS
				from m_sql_plan_cache
				where statement_string like '%'||p.rt_object||'%'
					and statement_string not like '%m_sql_plan_cache%')
			else
				(select max(max_execution_time * 1000) as MAX_EXEC_TIME_MS
				from m_sql_plan_cache
				where statement_string like '%'||p.it_object||'%'
					and statement_string not like '%m_sql_plan_cache%')
			end as MAX_EXEC_TIME,
		20 as SLA
	from $$SCHEMA$$.ERE_PARAMS p
		inner join $$SCHEMA$$.RT_ERE_INFO i on p.project_id = i.project_id
	where p.project_id = :project_id and p.enabled = 1;
end

--$$SCHEMA$$#SP_PERFORMANCE_INFO#PROCEDURE#1
CREATE PROCEDURE "$$SCHEMA$$"."SP_PERFORMANCE_INFO"(
	IN age_seconds INTEGER,
	IN username NVARCHAR(256)
)
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
BEGIN
	DECLARE monitoring_mode NVARCHAR(30);

	configuration = SELECT TOP 1 "VALUE" FROM "$$SCHEMA$$"."REPORT_SETTINGS" WHERE "KEY" = 'monitoring_mode';

	IF IS_EMPTY(:configuration) THEN
		monitoring_mode := 'statement';
	ELSE
		SELECT "VALUE" INTO monitoring_mode FROM :configuration;
	END IF;

	IF monitoring_mode = 'connection' THEN
			select
			u.USER_NAME as USER_NAMES,
			round(sm.TOTAL_MEMORY_USED_SIZE / (1024 * 1024 * 1024),2) as INDEXSERVER_ALLOCATED_GB,
			round(sm.ALLOCATION_LIMIT / (1024 * 1024 * 1024), 2) as ALLOCATION_LIMIT_GB,
			case when ss.process_cpu > 0 then ss.process_cpu else 0 end as PROCESS_CPU,
			ifnull(round(aet.THROUGHPUT, 0, ROUND_UP), 0) as TPS,
			ifnull(round(aet.AVG_EXEC_TIME), 0) as AVG_EXECUTION_TIME_MS,
			ifnull(MAX_EXEC_TIME/1000, 0) as MAX_EXEC_TIME,
			ifnull(round(aet.THROUGHPUT*:age_seconds, 0, ROUND_UP), 0) as ACTIVE_CONNECTION,
			sys_timestamp as TIME_STAMP,
			current_utctimestamp as UTC_TIMESTAMP,
			sm.TOTAL_MEMORY_USED_SIZE as INDEXSERVER_ALLOCATED_BYTES,
			sm.ALLOCATION_LIMIT as INDEXSERVER_ALLOCATION_LIMIT_BYTES
		from
			(select sum(TOTAL_MEMORY_USED_SIZE) as TOTAL_MEMORY_USED_SIZE,
					sum(ALLOCATION_LIMIT) as ALLOCATION_LIMIT
			 from M_SERVICE_MEMORY
			 where service_name = 'indexserver') sm
		inner join
			(select min(sys_timestamp) as sys_timestamp, to_integer(sum(process_cpu)/count(process_cpu)) as process_cpu
			 from sys.m_service_statistics ss
			 where service_name = 'indexserver') ss on 1=1
		inner join
			(select
				u.USER_NAME,
				count(c.user_name) as ACTIVE_CONNECTION
			from USERS u
				left join m_connections c on u.user_name = c.user_name and c.connection_status = 'RUNNING'
			where u.user_name = :username or :username = 'all_users'
			group by u.USER_NAME) u on 1=1
		left join
			(select
				user_name,
				round(sum(select_total_execution_time / 1000) / sum(select_execution_count),2) as AVG_EXEC_TIME,
				case
					when seconds_between(min(c.start_time), max(c.end_time)) > 0
					then round(sum(select_execution_count) / seconds_between(min(c.start_time),max(c.end_time)),2)
				else 0 end as THROUGHPUT
			from M_CONNECTIONS c
			inner join m_connection_statistics cs on c.connection_id = cs.connection_id
			where cs.select_execution_count >= 1
				and ADD_SECONDS(c.end_time, :age_seconds) >= CURRENT_TIMESTAMP
				and ADD_SECONDS(c.start_time, :age_seconds) >= CURRENT_TIMESTAMP
			group by c.user_name ) aet on u.user_name = aet.user_name
		left join
			(select
				c.user_name,
				max(select_max_execution_time) as MAX_EXEC_TIME
			from m_connections c
			inner join m_connection_statistics cs on c.connection_id = cs.connection_id
			where cs.select_execution_count >= 1
				and ADD_SECONDS(c.end_time, :age_seconds) >= CURRENT_TIMESTAMP
			group by c.user_name) met on met.user_name = u.user_name
		order by 4 desc;
	ELSE
		/* Retrieve memory statistics for index server */
		service_memory = SELECT SUM(TOTAL_MEMORY_USED_SIZE) AS TOTAL_MEMORY_USED_SIZE, SUM(ALLOCATION_LIMIT) AS ALLOCATION_LIMIT
		FROM M_SERVICE_MEMORY
		WHERE service_name = 'indexserver';

		/* Retrieve CPU statistics for index server */
		service_stats = SELECT TO_INTEGER(SUM(PROCESS_CPU)/COUNT(PROCESS_CPU)) AS PROCESS_CPU
		FROM M_SERVICE_STATISTICS
		WHERE service_name = 'indexserver';


		/* Retrieve Users */
		user_selection = SELECT USER_NAME
		FROM USERS
		WHERE USER_NAME = :username OR :username = 'all_users';

		/* Retrieve number of active connections */
		active_connections = SELECT l.USER_NAME, IFNULL(ACTIVE_CONNECTIONS,0) AS ACTIVE_CONNECTIONS
		FROM :user_selection l
		LEFT JOIN (
			SELECT USER_NAME, COUNT(*) AS ACTIVE_CONNECTIONS
			FROM M_CONNECTIONS
			WHERE CONNECTION_ID > 0 AND (USER_NAME = :username OR :username = 'all_users') AND IDLE_TIME <= :age_seconds*1000
			GROUP BY USER_NAME
		) r
		ON l.USER_NAME = r.USER_NAME;

		/* retrieve average execution time, max execution time and transactions per second */
		statements = SELECT USER_NAME, r.ACTIVE_CONNECTIONS, AVG_EXEC_TIME, MAX_EXEC_TIME, TPS
		FROM :active_connections l
		LEFT JOIN (
			SELECT DB_USER, ROUND(AVG(DURATION_MICROSEC)/1000) AS AVG_EXEC_TIME, ROUND(MAX(DURATION_MICROSEC)/1000) AS MAX_EXEC_TIME,
			COUNT(*)/:age_seconds AS TPS, COUNT(*) AS ACTIVE_CONNECTIONS
			FROM "SYS"."M_EXPENSIVE_STATEMENTS"
			WHERE START_TIME >= ADD_SECONDS(CURRENT_TIMESTAMP, -:age_seconds) AND
			(DB_USER = :username OR :username = 'all_users') AND
			(
				OPERATION = 'CALL'
				OR OPERATION = 'SELECT'
			)
			GROUP BY DB_USER
		) r
		ON l.USER_NAME  = r.DB_USER;

		SELECT
			USER_NAME,
			ROUND(TOTAL_MEMORY_USED_SIZE/(1024*1024*1024),2) AS INDEXSERVER_ALLOCATED_GB,
			ROUND(ALLOCATION_LIMIT/(1024*1024*1024),2) AS ALLOCATION_LIMIT_GB,
			CASE WHEN PROCESS_CPU > 0 THEN PROCESS_CPU ELSE 0 END AS PROCESS_CPU,
			IFNULL(ROUND(TPS, 0, ROUND_UP),0) AS TPS,
			IFNULL(ROUND(AVG_EXEC_TIME),0) AS AVG_EXECUTION_TIME_MS,
			IFNULL(ROUND(MAX_EXEC_TIME),0) AS MAX_EXEC_TIME,
			IFNULL(ACTIVE_CONNECTIONS, 0) AS ACTIVE_CONNECTION,
			CURRENT_TIMESTAMP AS TIME_STAMP,
			CURRENT_UTCTIMESTAMP AS UTC_TIMESTAMP,
			TOTAL_MEMORY_USED_SIZE as INDEXSERVER_ALLOCATED_BYTES,
			ALLOCATION_LIMIT as INDEXSERVER_ALLOCATION_LIMIT_BYTES
		FROM
			:statements, :service_memory, :service_stats;
	END IF;
END