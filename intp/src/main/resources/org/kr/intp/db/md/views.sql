--$$SCHEMA$$#V_PERFORMANCE_INFO#VIEW#0
CREATE VIEW $$SCHEMA$$.V_PERFORMANCE_INFO
(
	USER_NAME,
	INDEXSERVER_ALLOCATED_GB,
	ALLOCATION_LIMIT_GB,
	PROCESS_CPU,
	TPS,
	AVG_EXECUTION_TIME_MS,
	MAX_EXEC_TIME,
	ACTIVE_CONNECTION,
	TIME_STAMP,
	UTC_TIMESTAMP
) AS
select
	u.USER_NAME as USER_NAME,
	round(sm.TOTAL_MEMORY_USED_SIZE / (1024 * 1024 * 1024), 2) as INDEXSERVER_ALLOCATED_GB,
	round(sm.EFFECTIVE_ALLOCATION_LIMIT / (1024 * 1024 * 1024), 2) as INDEX_ALLOCATION_LIMIT_GB,
	case when ss.process_cpu > 0 then ss.process_cpu else 0 end as PROCESS_CPU,
	round(aet.THROUGHPUT) as TPS,
	round(aet.AVG_EXEC_TIME) as AVG_EXECUTION_TIME_MS,
	MAX_EXEC_TIME/1000 as MAX_EXEC_TIME,
	u.ACTIVE_CONNECTION as ACTIVE_CONNECTION,
	sys_timestamp as TIME_STAMP,
	current_utctimestamp
from sys.M_SERVICE_MEMORY sm
inner join sys.m_service_statistics ss on sm.SERVICE_NAME = ss.SERVICE_NAME
inner join
	(select
	u.USER_NAME,
	count(c.user_name) as ACTIVE_CONNECTION
	from USERS u
	left join m_connections c on u.user_name = c.user_name and c.connection_status = 'RUNNING'
	group by u.USER_NAME) u on 1=1
left join
	(select
		c.user_name,
		round(sum(select_total_execution_time / 1000) / sum(select_execution_count),2) as AVG_EXEC_TIME,
		case
			when seconds_between(min(c.start_time), max(c.end_time)) > 0
			then round(sum(select_execution_count) / seconds_between(min(c.start_time), max(c.end_time)),2)
			else 0
		end as THROUGHPUT
	from M_CONNECTIONS c
	inner join m_connection_statistics cs on c.connection_id = cs.connection_id
	where cs.select_execution_count >= 1
		and ADD_SECONDS(c.end_time, 360) >= CURRENT_TIMESTAMP
		and ADD_SECONDS(c.start_time, 360) >= CURRENT_TIMESTAMP
	group by c.user_name ) aet on u.user_name = aet.user_name
left join
	(select
		c.user_name,
		max(select_max_execution_time) as MAX_EXEC_TIME
	from m_connections c
	inner join m_connection_statistics cs on c.connection_id = cs.connection_id
	group by c.user_name) met on met.user_name = u.user_name
where sm.SERVICE_NAME = 'indexserver'
order by 4 desc
WITH READ ONLY

--$$SCHEMA$$#PROJECTS_INFO#VIEW#2
CREATE VIEW $$SCHEMA$$.PROJECTS_INFO
(
    PROJECT_ID,
    PROJECT_TYPE,
    VERSION,
    TABLE_NAME,
    TABLE_SCHEMA,
    PROCEDURE_SCHEMA,
    TYPE,
    PROCEDURE_NAME,
    KEY_NUMBER,
    KEY_NAME,
    KEY_DATATYPE,
    KEY_INITIAL,
    KEY_SUMMABLE,
    KEY_THRESHOLD,
    KEY_SEQUENTIAL
) AS
select
    T0.PROJECT_ID,
    T2.TYPE,
    T2.VERSION,
    T0.TABLE_NAME,
    T2.TABLE_SCHEMA,
    T2.PROC_SCHEMA,
    T3.TYPE,
    T3.PROCEDURE_NAME,
    T1.NUMBER,
    T1.KEY_NAME,
    T1.KEY_DATATYPE,
    T1.KEY_INITIAL,
    T1.KEY_SUMMABLE,
    T1.KEY_THRESHOLD,
    T1.KEY_SEQUENTIAL
from $$SCHEMA$$.RT_PROJECT_TABLES T0
inner join $$SCHEMA$$.RT_PROJECT_KEYS T1 on T0.PROJECT_ID = T1.PROJECT_ID
    and T0.NUMBER = T1.TABLE_NUMBER
inner join $$SCHEMA$$.RT_PROJECT_DEFINITIONS T2 on T2.PROJECT_ID = T0.PROJECT_ID
inner join $$SCHEMA$$.RT_PROJECT_PROCEDURES T3 on T0.PROJECT_ID = T3.PROJECT_ID
WITH READ ONLY

--$$SCHEMA$$#ALL_PRIVILEGS#VIEW#0
CREATE VIEW $$SCHEMA$$.ALL_PRIVILEGS
              (
              	GRANTEE_TYPE,
              	GRANTEE,
              	OBJECT_TYPE,
              	SUB_OBJECT_TYPE,
              	GRANTED_OBJECT
              ) AS
              SELECT
              	grantee_type,
              	grantee,
              	object_type,
              	sub_object_type,
              	granted_object
              FROM (SELECT * FROM
              		(SELECT NULL grantee_type, NULL grantee, 'USER' object_type, NULL sub_object_type, user_name granted_object
              		FROM SYS.USERS
                      	UNION
                      SELECT grantee_type, grantee, 'ROLE' object_type, NULL sub_object_type, role_name granted_object
                      FROM SYS.GRANTED_ROLES
                      	UNION
                      SELECT grantee_type, grantee, 'PRIVILEGE' object_type, object_type sub_object_type,
              			IFNULL(schema_name || ':','') || IFNULL(object_name || ':','') ||
              			IFNULL(column_name || ':','') || IFNULL(privilege,'') granted_object
                      FROM SYS.GRANTED_PRIVILEGES)
              	  ORDER BY grantee_type, grantee, object_type, sub_object_type,granted_object)
              WITH READ ONLY

--$$SCHEMA$$#INTP_GEN_PRIVILEGS#VIEW#0
CREATE VIEW $$SCHEMA$$.INTP_GEN_PRIVILEGS
(
	GRANTEE_TYPE,
	GRANTEE,
	OBJECT_TYPE,
	SUB_OBJECT_TYPE,
	GRANTED_OBJECT
) AS
SELECT
	GRANTEE_TYPE,
	GRANTEE,
	OBJECT_TYPE,
	SUB_OBJECT_TYPE,
	GRANTED_OBJECT
from $$SCHEMA$$.ALL_PRIVILEGS
where granted_object in
	(select ge_top.grantee
     from $$SCHEMA$$.ALL_PRIVILEGS as ge_top
     inner join
     	(select distinct ge.grantee as grantee
		 from $$SCHEMA$$.ALL_PRIVILEGS as ge
		 inner join $$SCHEMA$$.ALL_PRIVILEGS as gob on ge.granted_object = gob.grantee
		 where gob.granted_object LIKE '%INTP_GEN:%'
			AND gob.sub_object_type = 'SCHEMA'
			AND gob.grantee_type = 'ROLE' ) as role on ge_top.granted_object = role.grantee
				and ge_top.grantee_type = 'ROLE' )
WITH READ ONLY

--$$SCHEMA$$#INTP_PRIVILEGS#VIEW#0
CREATE VIEW $$SCHEMA$$.INTP_PRIVILEGS
(
	GRANTEE_TYPE,
	GRANTEE,
    OBJECT_TYPE,
	SUB_OBJECT_TYPE,
	GRANTED_OBJECT
) AS
SELECT
	GRANTEE_TYPE,
	GRANTEE,
	OBJECT_TYPE,
	SUB_OBJECT_TYPE,
	GRANTED_OBJECT
from $$SCHEMA$$.ALL_PRIVILEGS
where granted_object in
	(select ge_top.grantee
	 from $$SCHEMA$$.ALL_PRIVILEGS as ge_top
	 inner join
	 	(select distinct ge.grantee as grantee
	     from $$SCHEMA$$.ALL_PRIVILEGS as ge
		 inner join $$SCHEMA$$.ALL_PRIVILEGS as gob on ge.granted_object = gob.grantee
		 where gob.granted_object LIKE '%$$SCHEMA$$:%'
         	AND gob.sub_object_type = 'SCHEMA'
            AND gob.grantee_type = 'ROLE' ) as role on ge_top.granted_object = role.grantee
                and ge_top.grantee_type = 'ROLE' )
WITH READ ONLY