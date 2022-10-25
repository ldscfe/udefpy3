# -*- coding: utf-8 -*-
#
#
#  User-defined package
#
#

r"""
User-defined package
Support for Message v1.0
Copyright (c) 2018-2021 by Adam, All rights reserved.

This means that no one may use your work unless they obtain your permission.
This statement is not legally required, and failure to include it has no legal
significance. Since others may not use copyrighted works without the copyright
holder's permission, the statement is redundant.
"""

'''-----------------------------------------------------------------------------
 Name     : umess
 Purpose  : User-defined Message
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/05/29  Adam             Create

List:
 uerr                         # output error message
 log_txt                      # Write log info, default log_ubi_default.log

-----------------------------------------------------------------------------'''



'''-----------------------------------------------------------------------------
 Name     : uerr
 Purpose  : Output Error Message
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/16  Adam             Create

-----------------------------------------------------------------------------'''
def uerr(eid = 0):
    '''Output Error Message
    Error Message Define'''

    import ustr
    eid = ustr.str2num(eid)
    
    # Error Message Define
    d_err = { \
       0 : 'Default Error.' \
     ,1010 : 'Database connection error.' \
     ,1011 : 'No database definition.' \
     ,1012 : 'Source table does not exist.' \
     ,1015 : 'No data in the result set.' \
     ,1016 : 'Database write error.' \
     ,1020 : 'SQL Error.' \
     ,1021 : 'Not getting a valid connection type.' \
     ,1022 : 'ETL Error.' \
     ,1031 : 'umap not define.' \
     ,1032 : '_sy_col_type not define.' \
    }

    e_txt = d_err.get(eid)

    if e_txt:
        return 'Error-' + str(eid) + ' : ' + e_txt
    else:
        return 'Error-' + str(eid) + ' : ' + 'Error ID is not defined'

# uerr



'''-----------------------------------------------------------------------------
 Name     : log_txt
 Purpose  : Write log info, default log_ubi_default.log
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/06/22  Adam             Create

-----------------------------------------------------------------------------'''
def log_txt(type, mess, filename='log_ubi_default.log'):
    '''Write log info
    default log_ubi_default.log in local path or /tmp'''
    
    import time
    filename = str(filename)
    try:
        fhd = open(filename, mode='a')
    except Exception as e:
        filename = '/tmp/' + filename
        fhd = open(filename, mode='a')

    t1 = time.strftime("%Y%m%d%H%M%S", time.localtime())
    mess = str(mess).replace('\r', ' ').replace('\n', ' ')
    fhd.writelines(t1 + '|' + type + '|' + mess + '\n')
    fhd.close()

# log_txt
