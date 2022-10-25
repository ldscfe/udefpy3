# -*- coding: utf-8 -*-
#
#
#  User-defined package
#
#

r"""
User-defined package
Support for database SQL - Sys  v1.90
Copyright (c) 2018-2022 by Adam, All rights reserved.

This means that no one may use your work unless they obtain your permission.
This statement is not legally required, and failure to include it has no legal
significance. Since others may not use copyrighted works without the copyright
holder's permission, the statement is redundant.
"""

'''-----------------------------------------------------------------------------
 Name     : usql_sys
 Purpose  : User-defined database SQL - Sys
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/02  Adam             Create
 1.1        2020/05/20  Adam             rewrite
 1.2        2021/01/28  Adam             remove _ld
 1.5        2021/02/23  Adam             Add Transform
 1.51       2021/04/26  Adam             Add __mysql_oracle_ts : timestamp
 1.6        2021/05/25  Adam             Add __oracle_tab_col
 1.8        2021/08/19  Adam             Add __oracle_mysql_ts
 1.81       2021/09/06  LLC              Add __oracle_kw
 1.89       2022/07/21  LLC              Add __oracle_oracle_ts
 1.92       2022/09/20  Adam             Add __greenplum_load_data_csv[_gz]

-----------------------------------------------------------------------------'''
#### General
# c_kv
__db_c_kv           = "select key, val from bi.c_kv where 1=1 %wh%"


#### Table Exist
## Default
__tab_exist                   = '''
   select   count(*)
   from     %tab%
   limit 1
'''
## Oracle
__oracle_tab_exist            = '''
   select   count(*)
   from     %tab%
   where    rownum < 2
'''


#### Transform
__mysql_oracle_ts             = '''
    'varchar',   'varchar2',
    'bigint',    'number',
    'decimal',   'number',
    'double',    'number',
    'int',       'number',
    'float',     'number',
    'mediumint', 'number',
    'smallint',  'number',
    'tinyint',   'number',
    'char',      'varchar2',
    'datetime',  'date',
    'date',      'date',
    'time',      'date',
    'timestamp', 'date',
    'varchar2'
'''

__oracle_mysql_ts             = '''
    'varchar2',  'varchar',
    'number',    'decimal',
    'date',      'datetime',
    'varchar'
'''

__oracle_greenplum_ts         = '''
    'varchar2',  'varchar',
    'number',    'numeric',
    'date',      'timestamp',
    'varchar'
'''

__oracle_oracle_ts            ='''
    'varchar2',   'varchar2',
    'number',     'number',
    'date',       'date',
    'timestamp',  'date',
    'nvarchar2',  'nvarchar2',
    'varchar2'
'''

__oracle_clickhouse_ts       ='''
    'varchar2',   'varchar2',
    'number',     'Decimal',
    'date',       'Datetime',
    'timestamp',  'Datetime',
    'varchar2'
'''

#### Key Word, lower
__oracle_kw                   = [
    'comment', 
    'from',
    'to',
    'desc',
    'OracleDefault'
]

__mysql_kw                    = [
    
    'MySQLDefault'
]

####  Table Column Type
## format: id, name, type, field, col_len, col_len_octet, col_cset
#         1 LOG_NR_  bigint   bigint      19,0  19,0
#         2 TYPE_    varchar  varchar(64) 64    192   utf8

## Oracle
__oracle_tab_col     = '''
   select   column_id                   col_id,
            column_name                 col_name,
            data_type                   col_type,
            data_type                   col_field,
            case when data_precision is null then to_char(data_length) else to_char(data_precision)||','||to_char(data_scale) end col_len,
            data_length                 col_len_octet,
            character_set_name          col_cset
   from     all_tab_columns
   where    1=1
   and      lower(data_type)  not in (%data_type%)
   and      column_name       not in (%col%)
   and      owner ||'.'|| table_name = upper('%tab%')
   order by column_id
'''

## MySQL
__mysql_tab_col     = '''
   select
            ordinal_position            col_id,
            column_name                 col_name,
            data_type                   col_type,
            column_type                 col_field,
            case when character_maximum_length is null then concat(numeric_precision, ',', numeric_scale) else character_maximum_length end col_len,
            case when character_octet_length is null then concat(numeric_precision, ',', numeric_scale) else character_octet_length end     col_len_octet,
            character_set_name          col_cset
   from     information_schema.columns
   where    1=1
   and      lower(data_type)  not in (%data_type%)
   and      column_name       not in (%col%)
   and      concat(table_schema, '.', table_name) = '%tab%'
   order by ordinal_position
'''

####  Load Data

## Greenplum
__greenplum_load_data_csv_gz   = '''
   gzip -dc %FN% | PGPASSWORD=%PASSWD% psql -h %HOST% -U %USER% -d %DB% -w -c 'copy %TB% from STDIN with (FORMAT csv, header %HEADER%, escape '"'\\'"', encoding %ENCODING%)' 2>&1
'''
__greenplum_load_data_csv      = '''
   cat %FN% | PGPASSWORD=%PASSWD% psql -h %HOST% -U %USER% -d %DB% -w -c 'copy %TB% from STDIN with (FORMAT csv, header %HEADER%, escape '"'\\'"', encoding %ENCODING%)' 2>&1
'''

## Clickhouse
__clickhouse_load_data_csv_gz  = '''
   gzip -dc %FN% | clickhouse-client --query 'insert into %TB% FORMAT CSV'
'''


## file, table, ft, lt, cols, set
## MySQL
__mysql_load_data   = '''
   LOAD DATA LOCAL
     INFILE '%file%'
     INTO TABLE %table%
   FIELDS TERMINATED BY %ft%
   LINES TERMINATED BY %lt%
     (%cols%)
   %sets%
'''


#### Variable Lists
####
usql = {
 # General
 'db_c_kv'          : __db_c_kv,
 
 ## Table Exist
 'tab_exist'        : __tab_exist,
 'oracle_tab_exist' : __oracle_tab_exist,

 ##  Table Column Type
 'oracle_tab_col'   : __oracle_tab_col,
 'mysql_tab_col'    : __mysql_tab_col,

 ## Data Load
 'greenplum_load_data_csv_gz'  : __greenplum_load_data_csv_gz,
 'greenplum_load_data_csv'     : __greenplum_load_data_csv,
 'clickhouse_load_data_csv_gz' : __clickhouse_load_data_csv_gz,
 'mysql_load_data'             : __mysql_load_data,
 
 'Default'          : ''
}



umap = {
 ## Transform
 'mysql_oracle_ts'            : __mysql_oracle_ts.replace('\n', ' '),
 'oracle_greenplum_ts'        : __oracle_greenplum_ts.replace('\n', ' '),
 'oracle_clickhouse_ts'       : __oracle_clickhouse_ts.replace('\n', ' '),
 'oracle_mysql_ts'            : __oracle_mysql_ts.replace('\n', ' '),
 'oracle_oracle_ts'           : __oracle_oracle_ts.replace('\n', ' '),

 ## Key Word
 'oracle_kw'        : __oracle_kw,
 'mysql_kw'         : __mysql_kw,
 'NULL'             : ''
}



uenclose = {
 # MySQL
 'mysql'            : '`',
 'oracle'           : '"',

 'default'          : ''
}

