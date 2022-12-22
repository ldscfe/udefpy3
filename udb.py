# -*- coding: utf-8 -*-
#
#
#  User-defined package
#
#
r"""
User-defined package
Support for Database (DBMS) v1.92
Copyright (c) 2018-2022 by Adam, All rights reserved.

conn = db_conn(dict)                              connect to DB using host list
conn = db_conn_s(dict)                            connect to DB
dict = ukv(cur_r, k_type, k_id='', key='')        Get KV value
list = ucoln(cur)                                 Get Column Name
list = uresl(cur_r, sql, pl=1, CASE='')           Get Result to List
s_res = dbconnping(d_para)                        Test DB or Run SQL Get Top N
cols = ifss(db_type, n)                           Insert Field Separator String
s_sql = exec_sql(s_sql_code, d_para, rs=None, commit=True)
d_res = csql(d_para)                              Create Table

"""

import os
import re
import time

import ustr
import umess

import usql_sys


'''
-----------------------------------------------------------------------------
 Name     : udb
 Purpose  :
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/22  Adam             Create
 1.5        2020/11/18  Adam             Add ifss
 1.6        2021/01/06  Adam             Add exec_sql
 1.90       2022/8/17   Adam             db_conn --> db_conn_s, Add db_conn(connect to DB using host list)
 1.92       2022/12/22  Adam             Add Oracle DSN connect using oracledb

-----------------------------------------------------------------------------
'''

'''-----------------------------------------------------------------------------
 Name     : db_conn(dict)
 Purpose  : connect to DB using host list
 Author   : Adam
 Uses     : 

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.90       2022/08/16  Adam             Create

db {type,host,user,passwd,db,[port]}
host: ip or ip1, ip2, ip3...
-----------------------------------------------------------------------------'''
def db_conn(db, PRE=0):
    '''connect to DB using host list
    {type,host/host list,user,passwd[P2],db,[port]}'''

    _conn = None
    try:
        import random

        l_host = ustr.ulist(db.get('host', ''))
        # if host not availability, remove from [List], get host again.

        LOOP = len(l_host)
        for i in range(LOOP):
            s_host = random.choice(l_host)
            db['host'] = s_host
            #print('%s - %s' % (l_host, s_host)) # DEBUG
            try:
                _conn = db_conn_s(db, PRE)

                if _conn:
                    return _conn
            except Exception as e:
                err = str(e).replace('\r', ' ').replace('\n', ' ')
                print('%s - db_conn - conn: %s' % (usys.utime(usys.utime()), err))
                pass
            
            l_host.remove(s_host)

        return _conn

    except Exception as e:
        err = str(e).replace('\r', ' ').replace('\n', ' ')
        print('%s - db_conn: %s' % (usys.utime(usys.utime()), err))
        umess.log_txt('%s - db_conn: ', err)



'''-----------------------------------------------------------------------------
 Name     : db_conn_s(dict)
 Purpose  : connect to db
 Author   : Adam
 Uses     : crypt

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/22  Adam             Create
 1.1        2019/06/05  Adam             Add : passwd salt[PRE:], default PRE=0
 1.2        2019/06/25  Adam             Import : DB-API as if need
 1.3        2020/05/19  Adam             MySQL connect Add : local_infile=1
 1.5        2020/08/12  Adam             Add DB : greenplum
 1.6        2020/09/12  Adam             Add PRE : passwd salt, default PRE=2
 1.8        2020/11/18  Adam             Add DB : clickhouse
 1.81       2020/09/12  Adam             MODI PRE : passwd salt, default PRE=0
 1.82       2021/08/23  Adam             Fix : host like 'tcp://...'
 1.90       2022/08/17  Adam             db_conn --> db_conn_s
 1.92       2022/12/22  Adam             Add Oracle DSN connect using oracledb


db {type,host,user,passwd,db,[port]}
-----------------------------------------------------------------------------'''
def db_conn_s(db, PRE=0):
    '''connect to DB
    {type,host,user,passwd[P2],db,[port]}'''

    _conn = None
    try:
        PRE = ustr.str2num(PRE)                   # passwd pre-string, default PRE=0
        ## init, get dbtype, passwd
        db_type = db.get('type').lower()
        if db.get('passwd'):
            s_passwd = re.sub('\n', '',  os.popen("./crypt bidb " + db.get('passwd') + " U", "r").read())[PRE:]

        # para
        s_host = db.get('host', '')
        s_user = db.get('user', '')
        s_db   = db.get('db', '')


        # Oracle
        if db_type == "oracle":
            s_dsn   = db.get('dsn', '')
            if s_dsn:
                import oracledb
                _conn = oracledb.connect(user=s_user, password=s_passwd, dsn=s_dsn)
            else:
               import cx_Oracle as oc
               s_port = db.get('port', 1521)
               _conn = oc.connect(s_user + '/' + s_passwd + '@' + s_host + ':' + s_port + '/' + s_db)

        # postgreSQL & greenplum
        elif db_type in ["postgresql", "greenplum"]:
            import psycopg2 as pg
            s_port = db.get('port', 5432)
            _conn = pg.connect(host=s_host, user=s_user, password=s_passwd, database=s_db, port=s_port)

        # MySQL
        elif db_type ==  "mysql":
            import pymysql as my
            s_port = int(db.get('port', 3306))
            if 'tcp://' in s_host:
                s_host = s_host[6:]
            _conn = my.connect(host=s_host, user=s_user, passwd=s_passwd, db=s_db, port=s_port, local_infile=1)

        # SQLite
        elif db_type == "sqlite":
            import sqlite3 as s3
            _conn = s3.connect(s_db)

        # clickhouse
        elif db_type ==  "clickhouse":
            import clickhouse_driver as ch
            s_port = db.get('port', 9000)
            _conn = ch.connect(host=s_host, user=s_user, password=s_passwd, database=s_db, port=s_port)


        # SQLServer


        # NULL
        else:
            _conn = None
            umess.log_txt('db', umess.uerr(1021))

    except Exception as e:
        umess.log_txt('db', umess.uerr(1010))
        umess.log_txt('exc-db', str(e).replace('\r', ' ').replace('\n', ' '))

    return _conn



'''-----------------------------------------------------------------------------
 Name     : ukv(cur_r, k_type, k_id, key)
 Purpose  : Get KV value
 Author   : Adam
 Uses     : bi.c_kv

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/05/28  Adam             Create
 1.2        2019/06/25  Adam             add : k_id, k_type, key
 1.5        2020/12/17  Adam             MODIFY : k_type, key, k_id

-----------------------------------------------------------------------------'''
def ukv(cur_r, k_type, key='', k_id=''):
    d_val = {}
    if k_type:
        k_type = "k_type='%s' | " % k_type
    if k_id:
        k_id = "k_id='%s' | " % k_id
    if key:
        key = "key='%s' | " % key

    wh = k_id + k_type + key
    wh = 'and' + wh.strip('| ').replace('|', 'and')

    sql_s = "select key, val from bi.c_kv where %s" % (wh)
    try:
        cur_r.execute(sql_s)
        for rs1 in cur_r:
            d_val['key'] = rs1[0]
            d_val['val'] = rs1[1]
    except Exception as e:
        #print(e)
        umess.log_txt('exc-kv', str(e).replace('\r', ' ').replace('\n', ' '))
        return None


    return d_val

# ukv




'''-----------------------------------------------------------------------------
 Name     : ucoln(cur_r)
 Purpose  : Get Column Name
 Author   : Adam
 Uses     : list = ucoln(cur)

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/11/02  Adam             Create
 1.2        2021/08/20  Adam             M01: Add CASE

-----------------------------------------------------------------------------'''
def ucoln(cur_r, CASE='upper'):
    '''Get Column Name
    result = [list]'''

    l_val = []
    try:
        tp_col = cur_r.description
        for i in range(len(tp_col)):
            if CASE == 'lower':                             # M01: Add CASE
                l_val.append(tp_col[i][0].lower())
            elif CASE == 'upper':
                l_val.append(tp_col[i][0].upper())
            else:
                l_val.append(tp_col[i][0])

    except Exception as e:
        #print(e)
        #umess.log_txt('exc-udb-ucoln', str(e).replace('\r', ' ').replace('\n', ' '))
        return ''

    return l_val

# ucoln




'''-----------------------------------------------------------------------------
 Name     : uresl(cur_r, sql, pl=1, CASE='upper')
 Purpose  : Get Result to List
 Author   : Adam
 Uses     : list = uresl(sql, cur_r)

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/11/02  Adam             Create
 1.1        2021/07/02  Adam             M01 : pl is string --> number


def uresl(cur_r, sql, pl=1, CASE='lower'):

-----------------------------------------------------------------------------'''
def uresl(cur_r, sql, pl=1, CASE='upper'):
    '''Get Result to List
    [{},{}...]'''

    pl = ustr.str2num(pl, 1)            # M01 : pl is string --> number
    CASE = str(CASE)

    l_val = []
    try:
        cur_r.execute(sql)
        col_list = ucoln(cur_r, CASE)
        col_len = len(col_list)

        p = 1
        for rs1 in cur_r:
            col_tmp = {}
            for i in range(0, col_len):
                if rs1[i]:
                    col_tmp[col_list[i]] = str(rs1[i])
                else:
                    col_tmp[col_list[i]] = ''

            l_val.append(col_tmp)
            p += 1
            if p > pl:
                break
    except Exception as e:
        ustr.uout(str(e).split('\n')[0], 1)
        #umess.log_txt('exc-udb-uresl', str(e).replace('\r', ' ').replace('\n', ' '))
        pass

    return l_val

# uresl




'''-----------------------------------------------------------------------------
 Name     : dbconnping(d_para)
 Purpose  : Test DB or Run SQL Get Top N
 Author   : LLC
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/11/18  LLC              Create

d_para = {'db':'', 'sql':'', 'line':1}
-----------------------------------------------------------------------------'''
def dbconnping(d_para, PRE=0):
    '''Test DB or Run SQL Get Top N
    {'type':'connection-db', 'server':d_db, 'cmd':s_sql, 'cursor':'', 'result':1,
     'code':'9000', 'message':'ERROR - Default.', 'time_b':0, 'time_e':0,
     'secs':0, 'data':''}'''

    # User Defined
    import udb
    import ustr
    import usys

    time_b = usys.utime()
    # Command Parameter
    d_db = d_para['db']
    s_sql = d_para['sql']
    LINE = ustr.str2num(d_para['line'])

    d_return = {'type': 'connection', 'data': ''}

    d_res = {'type':'connection-db', 'server':d_db, 'cmd':s_sql, 'cursor':'', 'result':1, 'code':'0000', 'message':'ERROR - Default.', 'time_b':0, 'time_e':0, 'secs':0, 'data':''}
    cur = None
    try:
        # Get DB connect string(Dict)
        conn = udb.db_conn(d_db, PRE)
        cur  = conn.cursor()
        d_res['cursor'] = cur
        d_res['result'] = 0
        d_res['message'] = 'success.'
    except Exception as e:
        # print(e)
        d_res['code'] = '1010'
        d_res['message'] = 'ERROR - Database Connection Error. %s' % str(e)


    if s_sql:
        try:
            res = udb.uresl(cur, s_sql, LINE)
            d_res['data'] = res
        except Exception as e:
            d_res['code'] = '1020'
            d_res['message'] = 'ERROR - ' + str(e)

    time_e = usys.utime()
    d_res['time_b'] = usys.utime(time_b)
    d_res['time_e'] = usys.utime(time_e)
    d_res['secs'] = int((time_e - time_b)*100)/100

    try:
        cur.close()
        conn.close()
    except Exception as e:
        pass

    return d_res

# dbconnping-end




'''-----------------------------------------------------------------------------
 Name     : ifss(db_type, n)
 Purpose  : Insert Field Separator String
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/01/18  Adam             Create
 1.91       2022/8/24   Adam             udb --> ifss, Add greenplum, clickhouse db_type

-----------------------------------------------------------------------------'''
def ifss(db_type, n=1):
    '''Insert Field Separator String
    '''

    # cols
    db_type = db_type.strip(' ')[0:4].lower()

    # set insert separator format
    if   db_type == "orac":                       # Oracle
        sep1 = ":s"
    elif db_type == "post":                       # postgreSQL
        sep1 = "%s"
    elif db_type == "mysq":                       # MySQL
        sep1 = "%s"
    elif db_type == "sqli":                       # SQLite
        sep1 = "?"
    elif db_type == "gree":                       # Greenplum
        sep1 = "%s"
    elif db_type == "clic":                       # Clickhouse
        sep1 = ""

    return ((sep1 + ',')*n).strip(',')



'''-----------------------------------------------------------------------------
 Name     : exec_sql(s_sql_code, d_para, rs=None, commit=True)
 Purpose  : Get SQL or Exec SQL, commit/not commit
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/01/06  Adam             Create

# get SQL : (code, para{})
# exec SQL: (code, para{}, result count)
# exec SQL & not commit: (code, para{}, result count, False)
# para{} = type, connect, cursor
-----------------------------------------------------------------------------'''
def exec_sql(s_sql_code, d_para, rs=None, commit=True):
    '''Get SQL or Exec SQL, commit/not commit
    (s_sql_code, d_para, rs=None, commit=True)'''

    if not s_sql_code:
        return ''

    import ustr
    import usql_sys

    #ustr.uout("exec_sql : %s = %s" % (s_sql_code, d_para), 3)

    s_return = [0, '']
    s_sql = ''

    #conn    = d_para.get('connect')
    cur     = d_para.get('cursor')
    db_type = d_para.get('type', '').lower()

    # if s_sql_code is SQL
    if s_sql_code and len(s_sql_code.split(' ')) > 1:
        s_sql = s_sql_code
    else:
        s_sql = usql_sys.usql.get(db_type + '_' + s_sql_code, usql_sys.usql.get(s_sql_code))

    # if key-value exist, replace s_sql(key)
    if d_para:
        s_sql = ustr.urep(s_sql, d_para)

    # rs=0, commit
    if rs == 0:
        rs = -1
    if rs:
        try:
            cur.execute(s_sql)

            if cur.rowcount == rs or rs < 0:
                if commit:
                    cur.execute('commit')
            else:
                s_return = [2, "Error - Expected to return %s rows, actually return %s rows. SQL : %s" % (cur.rowcount, rs, s_sql)]
                cur.execute('rollback')
        except Exception as e:
            s_return = [1, "Error - SQL Execute Error. SQL : %s" % (s_sql)]
    else:
        s_return = [0, s_sql]

    return s_return

# END : exec_sql




'''-----------------------------------------------------------------------------
 Name     : csql(d_para)
 Purpose  : Get Create SQL
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/01/08  Adam             Create
 1.1        2021/02/25  Adam             Add Target Table Key Word Check. --> '_0'
 1.2        2021/03/09  Adam             Add Column length overlength processing --> '32000'
 1.5        2021/04/09  Adam             Create table Add owner.table
 1.8        2021/08/20  Adam             M01: string --> lower()

#  d_para{} = type_s, cur_s, owner_s, table_s, field_s; type_t, cur_t, owner_t, table_t, field_t; auto_ini
#
get col_b, To be Developed. ****
-----------------------------------------------------------------------------'''
def csql(d_para):
    '''Get Create SQL
    {type_s, cur_s, owner_s, table_s, field_s; type_t, cur_t, owner_t, table_t,
    field_t; auto_ini}'''

    import re
    import ustr
    import usql_sys
    ustr.uout("Create Table - Parameter : %s" % d_para, 8)

    COL_LEN = '8'

    d_return = {'result':1, 'message':'Default Error.'}

    d_vp = {}
    d_vp['type'] = d_para.get('type_t')
    d_vp['tab'] = '%s.%s' % (d_para['owner_t'], d_para['table_t'])
    if '.' in d_para['table_t']:
        d_vp['tab'] = d_para['table_t']

    s_sql = exec_sql('tab_exist', d_vp)[1]

    # get target DB Key Word List
    l_kw_t = usql_sys.umap.get( d_vp['type'].lower() + '_kw', 'NULL' )

    cur_t = d_para.get('cur_t')
    if not cur_t:
        d_return['message'] = 'Target Cursor Not Defined.'
        return d_return

    if uresl(cur_t, s_sql, 1):
        # Table exist
        d_return['result'] = 0
        d_return['message'] = ''
        return d_return

    d_vp.clear()
    d_vp['type'] = d_para.get('type_s')
    d_vp['tab'] = '%s.%s' % (d_para['owner_s'], d_para['table_s'])
    d_vp['data_type'] = d_para.get('notype', "'NULL'")
    d_vp['col'] = d_para.get('nocol', "'NULL'")
    s_sql = exec_sql('tab_col', d_vp)[1]

    #ustr.uout("Create Table - SQL : tab_col_s : %s" % s_sql, 8)

    cur_s = d_para.get('cur_s')
    if not cur_s:
        d_return['message'] = 'cur_s not defined.'
        return d_return

    l_col_s = uresl(cur_s, s_sql, 2000)
    ustr.uout('s_sql : %s' % s_sql, 8)
    ustr.uout('l_col_s : %s' % l_col_s, 8)
    # print('bigint --> %s' % ustr.ucase(s_rep, 'bigint'))

    s_rep_type = '%s_%s' % (d_para.get('type_s'), d_para.get('type_t'))
    s_rep = usql_sys.umap.get(s_rep_type + '_ts')
    if not s_rep:
        d_return['message'] = '%s_ts not defined.' % s_rep_type
        return d_return
    else:
        s_rep = s_rep.lower()

    #ustr.uout('map_type : %s, map : %s' % (s_rep_type, s_rep), 8)
    # print('bigint --> %s' % ustr.ucase(s_rep, 'bigint'))

    # get source cols
    s_col_def = d_para.get('field_s', '{}')

    s_col_b_def = ''
    s_col_e_def = ''
    if '}' in s_col_def:
        s_col_b_def = s_col_def.split('{')[0]
        s_col_e_def = s_col_def.split('}')[1]

    #### get col_b, To be Developed. ****
    if s_col_b_def:
        pass

    # get col_e
    if s_col_e_def:
        s_col_e = ''
        for c1 in s_col_e_def.split(';'):
            d1 = {}
            #c1 = ustr.usplit(c1)[1]
            c1 = ustr.usplit(c1, 1, ' ')[1]
            if '(' in c1:
                d1['COL_NAME'] = c1.split('(')[0]
                d1['COL_TYPE'] = 'default'
                d1['COL_LEN_OCTET'] = re.search('\(.*?\)', c1)[0].strip('()')
            else:
                d1['COL_NAME'] = c1
                d1['COL_TYPE'] = 'default'
                d1['COL_LEN_OCTET'] = COL_LEN
            l_col_s.append(d1)


    # get auto*.ini, default partition
    s_auto_ini = d_para.get('auto_ini', '')                 # M01: default = ''

    s_col_t = ''
    for col_line in l_col_s:
        # 20210201,OLD : col_name = ustr.uformat(col_line['COL_NAME']).strip(' ')
        col_name = col_line['COL_NAME'].strip(' ').lower()                                # M01: string --> lower()

        # IF col_name IN key list, --> + '_0'
        if col_name in l_kw_t:
            col_name = col_name + '_0'

        # column format : 30
        col_name = ' '*3 + col_name + ' '*(25-len(col_name)) + ' '*2

        # size > 32000
        l_size = ustr.str2num(col_line['COL_LEN_OCTET'])
        if not l_size or 'date' in col_line['COL_TYPE'].lower() or 'time' in col_line['COL_TYPE'].lower():              # M01: string --> lower()
            s_size = ''
        elif l_size > 32000 and d_para.get('type_t').lower() == 'oracle':
            s_size = '(32000)'
        else:
            s_size = '(%s)' % l_size
        l_size = str(l_size)

        s_col_t = s_col_t + col_name + ' ' + ustr.ucase(s_rep, col_line['COL_TYPE'].lower()) + s_size + ',\n'           # M01: string --> lower()

    s_col_t = s_col_t.replace('()', '').strip('\n,')

    table_t = '%s.%s' % (d_para['owner_t'], d_para['table_t'])
    if '.' in d_para['table_t']:
        table_t = d_para['table_t']

    s_sql_t = 'CREATE TABLE %s (\n%s\n)\n%s' % (table_t, s_col_t, s_auto_ini)
    s_sql_t = s_sql_t.strip(';,\n')

    ustr.uout("etl - 4: Create Table = %s" % (s_sql_t), 8)

    cur_t = d_para.get('cur_t')
    try:
        cur_t.execute(s_sql_t)
    except Exception as e:
        d_return['message'] = str(e)
        d_return['sql'] = s_sql_t.replace('\n', ' ')
        return d_return


    d_return['memo'] = "DB-00000: Create Table %s." % d_vp.get('tab')
    d_return['result'] = '0'
    d_return['message'] = 'Create Table %s.' % d_para.get('table_t')
    return d_return

# END : csql