create procedure $$SCHEMA$$.SP_PERFORMANCE_INFO (in age_seconds integer, in username nvarchar(256))
	language SQLSCRIPT
	reads sql data
as
begin
	select
		u.USER_NAME as USER_NAME,
		round(sm.TOTAL_MEMORY_USED_SIZE / (1024 * 1024 * 1024),2) as INDEXSERVER_ALLOCATED_GB,
		round(sm.ALLOCATION_LIMIT / (1024 * 1024 * 1024), 2) as ALLOCATION_LIMIT_GB,
		case when ss.process_cpu > 0 then ss.process_cpu else 0 end as PROCESS_CPU,
		ifnull(round(aet.THROUGHPUT), 0) as TPS,
		ifnull(round(aet.AVG_EXEC_TIME), 0) as AVG_EXECUTION_TIME_MS,
		ifnull(MAX_EXEC_TIME/1000, 0) as MAX_EXEC_TIME,
		u.ACTIVE_CONNECTION as ACTIVE_CONNECTION,
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
				then round(count(select_execution_count) / seconds_between(min(c.start_time),max(c.end_time)),2)
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
end