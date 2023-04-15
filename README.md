# udefpy3 - User Define Python Function
# Path: /usr/include/udefpy3
# Usage: import upy

##    v2.00
##    @2019-2022

#### v1.55       2022/5/9    Adam   mqlog, Add Clickhouse Insert SQL
#### v1.56       2022/6/27   Adam   mqlog, MIN/0/1 == '-1' --> MIN
#### v1.89       2022/7/21   LLC    usql_sys, Add oracle_oracle_ts
#### v1.90       2022/8/17   Adam   udb, db_conn --> db_conn_s, Add db_conn(connect to DB using host list)
#### v1.91       2022/8/24   Adam   udb, ifss, Add greenplum, clickhouse db_type
#### v1.92       2022/09/20  Adam   usql_sys, Add __greenplum_load_data_csv[_gz]
#### v2.00       2022/11/05  Adam   Release


## ucgi - Support for URL/CGI v2.1
###### ulog        - write log to URL
###### uform       - Get Web Form Data

## udb - Support for Database (DBMS) v1.90
###### db_conn     - connect to DB using host list
###### db_conn_s   - connect to DB
###### ukv         - Get KV value
###### ucoln       - Get Column Name
###### uresl       - Get Result to List
###### dbconnping  - Test DB or Run SQL Get Top N
###### ifss        - Insert Field Separator String
###### exec_sql    - Get SQL or Exec SQL, commit/not commit
###### csql        - Get Create SQL

## umess - Support for Message v1.0
###### uerr        - Output Error Message
###### log_txt     - Write log info

## umq - Support for Message Queue (MQ)  v1.56
###### mq_conn     - connect to MQ
###### mqct        - MQ Connection Test
###### mqlog       - Load MQ Log To DB

