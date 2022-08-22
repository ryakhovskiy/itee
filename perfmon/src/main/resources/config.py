############################################################
# Common config data
############################################################
# hdbclient python API path
hdbclient_path = "c:\\program files\\sap\\hdbclient"

#database connection info
db_host = "192.168.178.23"
db_port = 30015
db_user = "eintpadm"
db_pass = "xxx"

############################################################
# monitor config
############################################################
# memory statistics output file
output_file = "memstat.csv"

# allocators memory monitor query
mem_query = """
select COMPONENT, CATEGORY, DEPTH, INCLUSIVE_SIZE_IN_USE, CURRENT_UTCTIMESTAMP
from M_HEAP_MEMORY
where port = 30003 and DEPTH <= 2
"""

# interval to monitor allocators memory
mem_query_interval = 0.5
############################################################