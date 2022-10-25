# -*- coding: utf-8 -*-
#
#
#  User-defined package
#
#
r"""
User-defined package
Support for Message Queue (MQ)  v1.56
Copyright (c) 2018-2021 by Adam, All rights reserved.

This means that no one may use your work unless they obtain your permission.
This statement is not legally required, and failure to include it has no legal
significance. Since others may not use copyrighted works without the copyright
holder's permission, the statement is redundant.

v1.00 : pykafka
v1.52 : mqct  - 1.5        2021/10/02  Adam             M01: -1=Max, 0=Min, Other=Max
v1.52 : mqlog - 1.52       2021/10/02  Adam             M03: Fix many
v1.55 : mqlog - 1.55       2022/05/09  Adam             M04: Add Clickhouse Insert SQL
v1.56 : mqlog - 1.56       2022/06/27  Adam             M05: MIN/0/1 == '-1' --> MIN



"""


'''-----------------------------------------------------------------------------
 Name     : mq_conn(dict)
 Purpose  : connect to MQ
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/12/10  Adam             Create

{'result':0, 'code':'0000', 'message':'', 'conn':_conn}
-----------------------------------------------------------------------------'''
def mq_conn(mq):
    '''connect to MQ
    {'result':0, 'code':'0000', 'message':'', 'conn':_conn}'''

    _conn = None
    d_return = {'type':'mq_conn', 'result':1, 'code':'9000', 'message':'ERROR - Default.', 'conn':None}
    try:
        # para
        s_host = mq.get('host', '')
        s_type = mq.get('type', '').lower()
        s_topic= mq.get('topic', '')

        # Type
        if ':' in s_type:
            pass
        if ':2181' in s_host or 'zk' in s_type or 'zookeeper' in s_type:
            s_type = "kafka:zookeeper"
        elif ':9092' in s_host:
            s_type = "kafka"
        else:
            s_type = "kafka"

        # Connect
        if "kafka" in s_type:
            from pykafka import KafkaClient

            if s_type == "kafka":
                try:
                    _conn = KafkaClient(s_host)
                    d_return['conn'] = _conn
                except Exception as e:
                    d_return['code'] = '0001'
                    d_return['message'] = 'ERROR - Kafka : ' + str(e)
            else:
                try:
                    _conn = KafkaClient(zookeeper_hosts = s_host)
                    d_return['conn'] = _conn
                except Exception as e:
                    d_return['code'] = '0001'
                    d_return['message'] = 'ERROR - zookeeper : ' + str(e)

        # other
        #elif s_type ==  "????":


        # NULL
        else:
            d_return['code'] = '0002'
            d_return['message'] = 'ERROR - No MQ Type Definition.'

    except Exception as e:
        d_return['code'] = '0003'
        d_return['message'] = 'ERROR - No Definition. ' + str(e)

    d_return['result'] = 0
    d_return['code'] = '0000'
    d_return['message'] = ''

    return d_return
# mq_conn-end



'''-----------------------------------------------------------------------------
  Name     : mqct
  Purpose  : MQ Connection Test
  Author   : Adam

  Revisions:
  Ver        Date        Author           Description
  ---------  ----------  ---------------  ------------------------------------
  1.0        2020/12/10  Adam             Create
  1.2        2020/12/16  Adam             Parameter redefine
  1.5        2021/10/02  Adam             M01: -1=Max, 0=Min, Other=Max

format:
  mqct mqname(in mq.ini) [TOPIC[.CMD]|[.{N}]]
-----------------------------------------------------------------------------'''
def mqconnping(d_mq_ini):
    '''MQ Connection Test
    mqct mqname(in mq.ini) [TOPIC[.CMD]|[.{N}]]'''

    import sys
    # import time
    # User Defined
    import ustr
    import usys


    s_cmd = d_mq_ini.get('cmd')
    d_mq_ini.pop('cmd')

    d_return = {'type':'connection-mq', 'server':d_mq_ini, 'cmd':s_cmd, 'topic':'', 'offset_min':0, 'offset_max':0, 'offset':0, 'time_b':0, 'time_e':0, 'secs':0, 'result':1, 'code':'9000', 'message':'ERROR - Default.', 'data': ''}

    time_b = usys.utime()
    d_return['time_b'] = usys.utime(time_b)



    # MQ cursor
    try:
        # Get MQ connect string(Dict)
        #import umq
        #conn = umq.mq_conn(d_mq_ini)
        conn = mq_conn(d_mq_ini).get('conn')
        if not conn:
            d_return['code'] = '0001'
            d_return['message'] = 'ERROR - MQ Connection Error.'
            return d_return
    except Exception as e:
        d_return['code'] = '0002'
        d_return['message'] = 'ERROR - ' + str(e)
        return d_return

    try:
        if s_cmd:
            TOPIC=s_cmd.split(".")[0]
            #d_val['cmd'] = s_cmd
            d_return['topic'] = TOPIC
            CMD = f"conn.topics[b'{TOPIC}']"
            mqtp = eval(CMD)
            if '{' in s_cmd:
                 OFFSET_MIN = mqtp.earliest_available_offsets()[0].offset[0]
                 OFFSET_MAX = mqtp.latest_available_offsets()[0].offset[0]
                 OFFSET = int(s_cmd.split('{')[1].split('}')[0])

                 # M01: -1=Max, 0=Min, Other=Max
                 if OFFSET == 0:
                     OFFSET = -1
                 elif OFFSET == -1:
                     OFFSET = OFFSET_MAX - 1
                 elif OFFSET > 0:
                     OFFSET = max(OFFSET, OFFSET_MIN)
                     OFFSET = min(OFFSET, OFFSET_MAX-1)
                 else:
                     OFFSET = OFFSET_MAX - 1
                 d_return['offset_min'] = OFFSET_MIN
                 d_return['offset_max'] = OFFSET_MAX
                 res = mqtp.get_simple_consumer(consumer_group = 'test', auto_offset_reset = False, reset_offset_on_start=True, auto_commit_enable=False)
                 # print(OFFSET)
                 res.reset_offsets([(mqtp.partitions[0], OFFSET-1)])
                 # print(res.held_offsets.get(0))
                 val_a = res.consume()
                 d_return['offset'] = val_a.offset
                 d_return['data'] = val_a.value.decode()
                 pass
            else:
                if len(s_cmd.split('.')) == 1:
                    d_return['offset_min'] = mqtp.earliest_available_offsets()[0].offset[0]
                    d_return['offset_max'] = mqtp.latest_available_offsets()[0].offset[0]
                elif len(s_cmd.split('.')) > 1:
                    CMD =  'mqtp.' + s_cmd.split('.')[1] + '()'
                    d_return['cmd'] = CMD
                    d_return['data'] = eval(CMD)
                    #print("TOPIC : %s - %s" % (TOPIC, eval(CMD)) )

        else:
            d_return['brokers'] = conn.brokers
            d_return['topics'] = conn.topics


    except Exception as e:
        #print(e)
        d_return['code'] = '0003'
        d_return['message'] = 'ERROR - ' + str(e)
        return d_return


    d_return['result'] = 0
    d_return['code'] = '0000'
    d_return['message'] = 'success.'
    time_e = usys.utime()
    d_return['time_e'] = usys.utime(time_e)
    d_return['secs'] = int((time_e - time_b)*100)/100

    return d_return

# mqconnping-end




'''
------------------------------------------------------------------------------
  Name     : mqlog
  Purpose  : Load MQ Log To DB
  Author   : Adam

  Revisions:
  Ver        Date        Author           Description
  ---------  ----------  ---------------  ------------------------------------
  1.0        2021/08/12  Adam             Create
  1.2        2021/08/18  Adam             M01: replace r'\u0005' --> \x05
  1.2        2021/08/18  Adam             M00: upgrade
  1.5        2021/08/30  Adam             M02: Oracle Varchar2 < 3900
  1.51       2021/09/02  Adam             Fix: move mqlog --> umq
  1.52       2021/10/02  Adam             M03: Fix many
  1.55       2022/05/09  Adam             M04: Add Clickhouse Insert SQL
  1.56       2022/06/27  Adam             M05: MIN/0/1 == '-1' --> MIN

format:
  mqlog mq=mqname(in mq.ini) tp=TOPIC [commit=100 group=default offset=CURRENT]

Sample:
  nohup ./uagent "python3 mqlog mq=devmq tp=l_serv_monitor_io n=10000 group=default offset=1" 1 &
------------------------------------------------------------------------------
'''
# mq     (MQ Name in db.ini/mq.ini)
# tp     (topic)
# db     (DB Name in db.ini)
# table  (owner.table Name)
# col    (column list)
# n      = 1000
# group  = default
# offset = CURRENT
# commit = 100
# logfile= 'log_mq_load.log'
def mqlog(d_val):
    ## INIT
    import sys
    import ustr
    #import umq
    import usys
    import umess

    _time_b = usys.utime()

    d_res = {'type': 'MQ-LOG', 'result': 0, 'code': '0000', 'message': '', 'time_b': usys.utime(_time_b), 'time_e': ''}
    d_res.update(d_val)

    N         = int(d_res.get('n', '1000'))
    GROUP     = d_res.get('group', 'default')
    _OFFSET   = d_res.get('offset', None)
    COMMIT_N  = int(d_res.get('commit', '100'))
    LOGFILE   = d_res.get('logfile', 'log_mq_load.log')


    #
    s_mq_name = d_res.get('mq', '')
    s_topic   = d_res.get('tp', '')

    d_mq_ini = ustr.ini2dict(s_mq_name, 'mq.ini')
    if not d_mq_ini:
        d_mq_ini = ustr.ini2dict(s_mq_name, 'db.ini')
    if not d_mq_ini:
        d_res['code'] = '5010'
        d_res['result'] = 1
        d_res['message'] = 'ERROR - No section: %s' % s_mq_name
        return d_res


    # MQ cursor
    try:
        # Get MQ connect string(Dict)
        import umq
        conn_mq = umq.mq_conn(d_mq_ini).get('conn')
        if not conn_mq:
            d_res['code'] = '2010'
            d_res['result'] = 1
            d_res['message'] = 'ERROR - MQ Connection Error.'
            return d_res
    except Exception as e:
        d_res['code'] = '2010'
        d_res['result'] = 1
        d_res['message'] = 'ERROR - ' + str(e)
        return d_res


    _tmp = f"conn_mq.topics[b'{s_topic}']"
    MQTP = eval(_tmp)



    S_DB_TYPE=ustr.ini2dict(d_res.get('db')).get('type')
    try:
        import udb
        conn_db = udb.db_conn(ustr.ini2dict(d_res.get('db')), 2)
        cur_db = conn_db.cursor()

    except Exception as e:
        d_res['code'] = '1010'
        d_res['result'] = 1
        d_res['message'] = 'ERROR - ' + str(e)
        return d_res


    # Insert Format          # M04: Add Clickhouse Insert SQL
    S_COMMIT_FLAG = 1        # =1 commit
    if 'clickhouse' in S_DB_TYPE.lower():
        S_COMMIT_FLAG = 0
        orcl_sql = 'insert into %table%(%col%) values '
    else:
        orcl_sql = 'insert into %table%(%col%) values(%val%)'

    _tmp = d_res.get('col','')
    _tmp = ':1, ' * (_tmp.count(',')+1)
    _tmp = _tmp.strip(', ')
    orcl_sql = orcl_sql.replace('%table%', d_res.get('table',''))
    orcl_sql = orcl_sql.replace('%col%', d_res.get('col',''))
    orcl_sql = orcl_sql.replace('%val%', _tmp)



    OFFSET_MIN = MQTP.earliest_available_offsets()[0].offset[0]
    OFFSET_MAX = MQTP.latest_available_offsets()[0].offset[0]
    ROWS       = OFFSET_MAX - OFFSET_MIN - 1                # M03: -1

    d_res['offset_min'] = OFFSET_MIN
    d_res['offset_max'] = OFFSET_MAX
    #print(type(MQTP))
    #print("%s %s %s %s" % (OFFSET, OFFSET_MIN, OFFSET_MAX, ROWS))

    #MQ_CUR = MQTP.get_simple_consumer(consumer_group = GROUP, auto_offset_reset = False, reset_offset_on_start=True, auto_commit_enable=False)
    MQ_CUR = MQTP.get_simple_consumer(consumer_group = GROUP, auto_offset_reset = True, reset_offset_on_start=False, auto_commit_enable=True)


    if _OFFSET:
        if _OFFSET.upper() == 'MIN' or _OFFSET == '0' or _OFFSET == '1':  # M05 : MIN/0/1 == '-1' --> 0
            OFFSET = -1                                               # M03: reset-1 --> -2
        elif _OFFSET.upper() == 'MAX' or _OFFSET == '-1':             # M03: _OFFSET == '-1' --> MAX -1
            OFFSET = OFFSET_MAX -1
        else:
            OFFSET = int(ustr.str2num(_OFFSET))
        if OFFSET not in range(OFFSET_MIN, OFFSET_MAX) and OFFSET > 0: # M03: OFFSET > 0
            d_res['code'] = '5020'
            d_res['result'] = 1
            d_res['message'] = 'ERROR - OFFSET Not In Range(%s, %s).' % (OFFSET_MIN, OFFSET_MAX)
            return d_res
        MQ_CUR.reset_offsets([(MQTP.partitions[0], OFFSET-1)])

    i = 0
    l_rs = []
    for s_mq in MQ_CUR:
        if s_mq:
            i = i + 1
            _d_tmp = ustr.udict(s_mq.value.decode())
            _l_tmp = []
            for _s_tmp in d_res.get('col','').split(','):
                _s_tmp1 = _d_tmp.get(_s_tmp.strip(' '), '')
                _s_tmp1 = _s_tmp1.replace(r'\u0005', '\x05')                   # M01: replace r'\u0005' --> \x05
                if 'oracle' in S_DB_TYPE.lower():
                    _s_tmp1 = _s_tmp1[:3900]                                   # M02: Oracle Varchar2 < 4000
                _l_tmp.append(_s_tmp1)
            _tp = tuple(_l_tmp)
            l_rs.append(_tp)


            if i % COMMIT_N == 0 or i >= N:
                try:
                    cur_db.executemany(orcl_sql, l_rs)
                    l_rs = []
                    if i % (COMMIT_N * 10) == 0:
                        d_res['rs'] = i
                        d_res['offset'] = s_mq.offset
                        umess.log_txt('MQ-...', d_res, LOGFILE)
                    if S_COMMIT_FLAG:
                        cur_db.execute('commit')
                except Exception as e:
                    # if ignore, save point ignore_min, update ignore_min.
                    
                    # reset Current OFFSER
                    _current_os = i % COMMIT_N
                    if _current_os == 0:
                        _current_os = COMMIT_N
                    OFFSET_MIN = MQTP.earliest_available_offsets()[0].offset[0]
                    _OFFSET = max(s_mq.offset - _current_os, OFFSET_MIN)
                    MQ_CUR.reset_offsets([(MQTP.partitions[0], _OFFSET)])       # M04: -1 ????
                    MQ_CUR.stop();
                    d_res['code'] = '1016'
                    d_res['result'] = 1
                    d_res['message'] = 'ERROR - DB Err - %s.' % (str(e))
                    return d_res
                #print('rs: %s, offset: %s, secs: %s' % (i, s_mq.offset, usys.utime()- _time_b))
            if i >= N:
                break

    ## DEBUG: reset Current OFFSER
    #_current_os = i % COMMIT_N
    #if _current_os == 0:
    #    _current_os = COMMIT_N
    #_OFFSET = max(s_mq.offset - _current_os, OFFSET_MIN)
    #MQ_CUR.reset_offsets([(MQTP.partitions[0], _OFFSET)])
    ## DEBUG: reset Current OFFSER - END
    d_res['rs'] = i
    d_res['offset'] = s_mq.offset

    MQ_CUR.stop();
    cur_db.close()
    conn_db.close();


    _time_e = usys.utime()
    d_res['time_e'] = usys.utime(_time_e)
    d_res['secs'] = round(_time_e - _time_b, 2)

    d_res['code'] = '0000'
    d_res['result'] = 0
    d_res['message'] = ''
    return d_res
