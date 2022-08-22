--status_clean
insert into $$SCHEMA$$.rt_status_desc (id, name, description) values (-10, 'C', 'CLEAN')

--status_triggered
insert into $$SCHEMA$$.rt_status_desc (id, name, description) values (0, 'T', 'TRIGGERED')

--status_waiting
insert into $$SCHEMA$$.rt_status_desc (id, name, description) values (10, 'W', 'WAITING')

--status_started
insert into $$SCHEMA$$.rt_status_desc (id, name, description) values (20, 'S', 'STARTED')

--status_aggregating
insert into $$SCHEMA$$.rt_status_desc (id, name, description) values (30, 'A', 'AGGREGATING')

--status_processing
insert into $$SCHEMA$$.rt_status_desc (id, name, description) values (40, 'P', 'PROCESSED')

--status_finished
insert into $$SCHEMA$$.rt_status_desc (id, name, description) values (50, 'F', 'FINISHED')

--status_error
insert into intp.rt_status_desc (id, name, description) values (100, 'E', 'ERROR')

--default categories
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(0,null,'UNCATEGORISED')
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(1,0,'MM')
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(2,0,'FI')
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(3,0,'PM')
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(4,2,'LC')
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(5,2,'CA')
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(6,5,'CO')
insert into "$$SCHEMA$$"."RT_CATEGORIES" values(7,1,'IM')
