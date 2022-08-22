--SET_RUNNING##0
CREATE PROCEDURE $$SCHEMA$$.SET_RUNNING(in instance_id nvarchar(3)) AS
	status integer;
begin
	declare INTP_ERROR condition for SQL_ERROR_CODE 10099;
	lock table $$SCHEMA$$.rt_server in exclusive mode nowait;
	select top 1 status into status from $$SCHEMA$$.rt_server where instance_id = :instance_id;
	if :status = 1 then
		signal INTP_ERROR set message_text = 'INTP is already running!';
	end if;
	update $$SCHEMA$$.RT_SERVER set status = 1 where instance_id = :instance_id;
end

--ERE_STATUS##1
create procedure $$SCHEMA$$.ERE_STATUS(in project_id nvarchar(64))
language sqlscript reads sql data
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