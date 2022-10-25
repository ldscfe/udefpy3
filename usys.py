# -*- coding: utf-8 -*-
#
#
#  User-defined package
#
#
r"""
User-defined package
Support for SYS
Copyright (c) 2018-2021 by Adam, All rights reserved.

This means that no one may use your work unless they obtain your permission.
This statement is not legally required, and failure to include it has no legal
significance. Since others may not use copyrighted works without the copyright
holder's permission, the statement is redundant.
"""

'''-----------------------------------------------------------------------------
 Name     : usys
 Purpose  : User-defined SYS
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/11/12  Adam             Create


List:
 uid                          # get UUID
 upid                         # get pid,ppid
 uhost                        # get hostname
 uuser                        # get username
 utime                        # get time now, 1.time 2.string
 utime_offset                 # get time now - offset(secs)
 cp                           # Command Parameter, return dict
 md5                          # md5
-----------------------------------------------------------------------------'''





'''-----------------------------------------------------------------------------
 Name     : uid
 Purpose  : get UUID
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/16  Adam             Create

-----------------------------------------------------------------------------'''
def uid():
    try:
        import uuid
        val = str(uuid.uuid1()).replace('-', '')
    except Exception as e:
        val = ''

    return val



'''-----------------------------------------------------------------------------
 Name     : upid
 Purpose  : get pid,ppid
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/01/16  Adam             Create

-----------------------------------------------------------------------------'''
def upid(flag = None):
    try:
        import os
        if not flag:
            val = str(os.getpid()) + ',' + str(os.getppid())
        else:
            val = str(os.getpid())
    except Exception as e:
        val = ''

    return val



'''-----------------------------------------------------------------------------
 Name     : uhost
 Purpose  : get hostname
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/12/23  Adam             Create

-----------------------------------------------------------------------------'''
def uhost():
    try:
        import socket
        val = socket.gethostname()
    except Exception as e:
        val = ''

    return val


'''-----------------------------------------------------------------------------
 Name     : uip
 Purpose  : get IP
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/12/23  Adam             Create

-----------------------------------------------------------------------------'''
def uip():
    res = None
    try:
        import os
        r1 = os.popen("ping " + uhost() + " -c1", "r").read()
        if r1:
            res = r1.split('(')[1].split(')')[0]
    except Exception as e:
        pass
    
    return res


'''-----------------------------------------------------------------------------
 Name     : uuser
 Purpose  : get username
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/12/23  Adam             Create

-----------------------------------------------------------------------------'''
def uuser():
    try:
        import getpass
        val = getpass.getuser()
    except Exception as e:
        val = ''

    return val




'''-----------------------------------------------------------------------------
 Name     : utime
 Purpose  : get time now
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/16  Adam             Create
 1.1        2022/10/10  Adam             Add utime_offset

t1 = time.time()    # time format
-----------------------------------------------------------------------------'''
def utime(t1=None):
    val = 0
    try:
        import time
        if t1:
            val = time.strftime("%Y%m%d%H%M%S", time.localtime(t1))
        else:
            val = time.time()
    except Exception as e:
        print(e)
        return t1

    return val

def utime_offset(offset=0):
    val = 0
    try:
        import time
        val = time.time() - offset
    except Exception as e:
        print(e)
        return None

    return val



'''-----------------------------------------------------------------------------
 Name     : cp
 Purpose  : Command Parameter, return dict
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/12/14  Adam             Create
 1.1        2021/03/25  Adam             Add : Case Sensitive --> key

-----------------------------------------------------------------------------'''
def cp(CPS=1, HELP='Too few parameters.', LOWER='yes'):
    
    import sys
    # Command Parameter
    if len(sys.argv) <= CPS:
        print(HELP)
        sys.exit(1)

    # Parameter --> Dict
    d_val = {}
    for i in range(1, len(sys.argv)):
        if '=' in sys.argv[i]:
            key, value = sys.argv[i].split('=', 1)
            if LOWER.lower() == 'yes':
                key = key.lower()
            d_val[key] = value
        else:
            d_val[i] = sys.argv[i]
        
    return d_val





'''-----------------------------------------------------------------------------
 Name     : md5
 Purpose  : md5
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/12/15  Adam             Create

-----------------------------------------------------------------------------'''
def md5(str1, LOWER='yes'):
    
    import hashlib
    
    s_res = ''
    if str1:
        s_res = hashlib.md5(str1.encode(encoding='UTF-8')).hexdigest()
        if str(LOWER).lower() == 'yes':
            #s_res = s_res.lower()
            pass
        else:
            s_res = s_res.upper()
        
    return s_res

