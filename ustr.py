# -*- coding: utf-8 -*-
#
#
#  User-defined package
#
#
r"""
User-defined package
Support for string v2.4
Copyright (c) 2018-2021 by Adam, All rights reserved.

This means that no one may use your work unless they obtain your permission.
This statement is not legally required, and failure to include it has no legal
significance. Since others may not use copyrighted works without the copyright
holder's permission, the statement is redundant.
"""

import sys
import re


'''-----------------------------------------------------------------------------
 Name     : ustr
 Purpose  : User-defined string package
 Author   : Adam
 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/22  Adam             Create
 2.0        2019/12/23  Adam             Fix
 2.1        2021/01/11  Adam             urep(str1, d_val)
 2.2        2021/01/12  Adam             getdval(d_val, p)
 2.3        2021/05/12  Adam             ure(s1, c_s="'", s_t=" ")
 2.4        2021/08/29  Adam             udict, udict...

 List:
  uprint(s)                                                 # s --> string, \r OR \n --> ' '
  uout(s, level, group)                                     # if level >= DEBUG, output s.
  udict(string, ':', ',', '[lower|upper]')                  # Convert a string to a dictionary
  ulist(str, p, n)=(,'')                                    # string to list format
  ini2dict(s, s)=(db.ini)                                   # Convert a instance(.ini) to a dictionary
  range2str(s, s)=(-)                                       # range to string: 0-5 --> 0,1,2,3,4,5
  str2num(s, n)=('')                                        # string to number
  ucase(str|list, key)                                      # case when for str|list ****Modify, rewrite
  unvl(obj1, r1, r2) = (obj1)                               # if obj1 is null, then r1, or r2
  ujson(code, header, message, data)                        # return json
  urep(str1, d_val, sc=None, s_esc="'")                     # replace str1(%XX%) from d_val(DICT)
  getdval(d_val, p)                                         # get value from dict:d_val, p=1,2,3... 0=max
  getlval(l_val, p)                                         # get value from list:l_val, p=1,2,3... 0=max
  uformat(str, len=30, type='r', gap=1)                     # format string: r/c/l=1/0/-1
  usplit(s1, l1=2, p1=',', t1='r')                          # split string
  valconv(l_dat, type=None)                                 # Convert special strings and dates in tuple or list
  ure(s1, c_s="'", s_t=" ")                                 # Purpose : char replace

-----------------------------------------------------------------------------'''
DEBUG = 0


def uprint(mess):
    global DEBUG
    if DEBUG==0:
        try:
            print(str(mess).replace('\r', ' ').replace('\n', ' '))
            pass
        except Exception as e:
            print(str(e).replace('\r', ' ').replace('\n', ' '))

# uprint


'''-----------------------------------------------------------------------------
 Name     : uout
 Purpose  : output message
 Author   : Adam
 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/16  Adam             Create
 1.1        2019/12/23  Adam             DEBUG, GROUP Default value
 1.2        2020/12/18  Adam             DEBUG, GROUP Default value

-----------------------------------------------------------------------------'''
def uout(obj1, d1 = 1):
    try:
        global DEBUG
        if str(d1) <= str(DEBUG):
            print(obj1)
    except Exception as e:
        uprint(e)

# uout



'''-----------------------------------------------------------------------------
 Name     : udict
 Purpose  : Convert a string to a dictionary, flag : lower, upper, ''
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/22  Adam             Create
 1.2        2019/07/01  Adam             str2dict --> udict, rewrite
 1.5        2021/08/02  Adam             M01: Add strip {}
-----------------------------------------------------------------------------'''
def udict(p_str1, type_is = ':', type_part = ',', flag = 'normal'):
    try:
        str1 = p_str1.strip('{} ')
        str1 = str1.strip('\t\n '+type_part) + type_part
        size = len(str1)

        d_str = {}
        pb = ''
        key = val = c1 = ''

        (key_type, val_type) = (1, 0)
        for i in range(0, size):
            c1 = str1[i:i+1]
            if c1 in ['"', "'"]:
                if pb == c1:
                    pb = c1 = ''
                elif pb == '':
                    pb = c1
                    c1 = ''
            elif pb:
                pass
            elif c1 == type_is:
                (key_type, val_type) = (0, 1)
                c1 = ''
            elif c1 == type_part:
                if flag.lower()[0:5] == 'lower':
                    d_str[key.strip('\t ').lower()] = val.strip('\t ')
                elif flag.lower()[0:5] == 'upper':
                    d_str[key.strip('\t ').upper()] = val.strip('\t ')
                else:
                    d_str[key.strip('\t ')] = val.strip('\t ')

                (key_type, val_type) = (1, 0)
                key = val = c1 = ''

            if key_type:
                key += c1
            else:
                val += c1

        return d_str
    except Exception as e:
        uprint(e)
        return {}

# udict




'''-----------------------------------------------------------------------------
 Name     : ulist(str, p, n)=(,'')
 Purpose  : string to list format
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/26  Adam             Create
 1.2        2019/06/27  Adam             str2list --> ulist
 2.0        2019/12/10  Adam             M01: list.num = 1
 2.0        2019/12/10  Adam             M02: 'a', 1 --> 'a', '1'

-----------------------------------------------------------------------------'''
def ulist(ps1, separator = ',', isnull = ''):
    try:
        s1 = re.sub(r"\s*,\s*", "'"+separator+"'", ps1).strip()
        if s1:
            s1 = "'" + s1 + "'"
            s1 = s1.replace("''", "'")                      # M02: 'a', 1 --> 'a', '1'
        else:
            s1 = "'"+isnull+"'"

        if ',' in s1:
            return list(eval(s1))
        else:                                               # M01: list.num = 1
            lval = []
            lval.append(s1.strip("'"))
            return lval
    except Exception as e:
        uprint(e)
        lval = []
        lval.append(ps1)
        return lval

# ulist




'''-----------------------------------------------------------------------------
 Name     : ini2dict(s, s)=(db.ini)
 Purpose  : Convert a instance(.ini) to a dictionary
 Author   : Adam
 Uses     : ConfigParser

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/25  Adam             Create

-----------------------------------------------------------------------------'''
def ini2dict(p_opt, fn = 'db.ini'):
    try:

        # for read cfg, ini
        if sys.version[0] == '3':
            import configparser as cfg
        elif sys.version[0] == '2':
            import ConfigParser as cfg

        dbini = cfg.ConfigParser()
        dbini.read(fn)

        _dict = {}

        for section in dbini.options(p_opt):
             _dict[section] = dbini.get(p_opt, section)

        return _dict
    except Exception as e:
        return {}

#ini2dict



'''-----------------------------------------------------------------------------
 Name     : range2str(s, s)=(-)
 Purpose  : range to string: 0-5 --> 0,1,2,3,4,5
 Author   : Adam
 Uses     : re

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/25  Adam             Create

-----------------------------------------------------------------------------'''
def range2str(range1, separator = ''):
    #default format: [0-9]*<char...>[0-9]*
    if separator == '':
        separator = '-'
        range1 = re.sub('\D', separator, range1)
    try:
        str1 = ''
        f, l = range1.split(separator, 1)
        for i in range(int(f), int(l)):
            str1 = str1 + str(i) + ','
        str1 = str1 + l
        return str1
    except Exception as e:
        uprint(e)
        return range1

# range2str



'''-----------------------------------------------------------------------------
 Name     : str2num(s, n)=('')
 Purpose  : string to number
 Author   : Adam
 Uses     : re

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/03/26  Adam             Create
 1.1        2019/03/26  Adam             Add abs

-----------------------------------------------------------------------------'''
def str2num(str1, value = None):
    try:
        nf = float(str1)
        ni = int(nf)
        if abs(abs(nf) - abs(ni)) < 1e-16:
            return ni
        else:
            return nf

    except Exception as e:
        #uprint(e)
        return value

#to_num





'''-----------------------------------------------------------------------------
 Name     : ucase(str|list, key)
 Purpose  : case when for str|list
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/08  Adam             Create

-----------------------------------------------------------------------------'''
def ucase(p1, p_key):
    #print("ucase: ", p1, p_key, type(p1), type(p_key))
    if not p1:
      return None

    try:
        if type(p1) == str:
            l1 = list(eval(p1))
        else:
            l1 = p1

        l1_len = int(len(l1)/2)
        for i in range(0, l1_len):
            if l1[2*i] == p_key:
                return l1[2*i+1]

        return l1[2*l1_len]
    except Exception as e:
        uprint(e)
        return None

# ucase




'''-----------------------------------------------------------------------------
 Name     : unvl(obj1, r1, r2) = (obj1)
 Purpose  : if obj1 is null, then r1, or r2
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/09  Adam             Create

-----------------------------------------------------------------------------'''
def unvl(obj1, r1, r2 = None):
    try:
        if obj1:
            if r2:
                return r2
            else:
                return obj1
        else:
            return r1
    except Exception as e:
        uprint(e)
        return obj1

# unvl


'''-----------------------------------------------------------------------------
 Name     : ujson(code, header, data, message) = (None)
 Purpose  :
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/05/22  Adam             Create

-----------------------------------------------------------------------------'''
def ujson(code, header, data, message = None):
    try:
        import json

        d_json = {}
        d_json['code'] = code
        d_json['header'] = header
        if message:
            d_json['message'] = message
        d_json['data'] = data

        return json.dumps(d_json, ensure_ascii=False)

    except Exception as e:
        uprint(e)
        return None

# ujson



'''-----------------------------------------------------------------------------
 Name     : urep(str1, d_val, sc=None, s_esc="'")
 Purpose  : replace str1's %key% from dict:d_val
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2020/12/21  Adam             Create
 1.1        2021/01/11  Adam             (d_val, str1) --> (str1, d_val)
 1.2        2021/01/19  Adam             (d_val, str1) --> (str1, d_val, sc=None, s_esc="'")
 1.5        2021/07/28  Adam             fix sc

sc    = special char, --> s_esc + sc
s_esc = escape char
-----------------------------------------------------------------------------'''
def urep(str1, d_val, sc=None, s_esc="'", P='%'):
    if not str1:
        return str1
    if sc:
        for ky in d_val:
            try:
                d_val[ky] = d_val[ky].replace(sc, s_esc + sc)
            except Exception as e:
                pass
    try:
        for ky in d_val:
            s_ky = P + str(ky) + P
            str1 = str1.replace(s_ky, str(d_val[ky]))

        return str1

    except Exception as e:
        uprint(e)
        return str1

# urep




'''-----------------------------------------------------------------------------
 Name     : getdval(d_val, p)
 Purpose  : get value from dict:d_val, p=1,2,3... 0=max
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/01/12  Adam             Create

-----------------------------------------------------------------------------'''
def getdval(d_val, p=1):
    i=1
    try:
        for ky in d_val:
            if p==i:
                return d_val[ky]
            i += 1
        return d_val[ky]

    except Exception as e:
        return ''

# getdval




'''-----------------------------------------------------------------------------
 Name     : getlval(l_val, p)
 Purpose  : get value from list:l_val, p=1,2,3... 0=max
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/03/25  Adam             Create

-----------------------------------------------------------------------------'''
def getlval(l_val, p=1):
    i=1

    try:
        for val in l_val:
            if p==i:
                return val
            i += 1
        if p == 0:
            return val
        else:
            return None
    except Exception as e:
        return None

# getlval




'''-----------------------------------------------------------------------------
 Name     : uformat(s1, l1=30, t1='l', g1=1)
 Purpose  : format string: r/c/l=1/0/-1
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/01/25  Adam             Create

-----------------------------------------------------------------------------'''
def uformat(s1, l1=30, t1='l', g1=1):

    s1 = str(s1)
    if len(s1) >= l1:
        return s1

    t1 = t1.lower()
    if t1 == 'r' or t1 == '1':
        s1 = ' ' * (l1 - len(s1)) + s1
    elif t1 == 'c' or t1 == '0':
        pass
    else:
        s1 = s1 + ' ' * (l1 - len(s1))

    return s1
# uformat




'''-----------------------------------------------------------------------------
 Name     : usplit(s1, l1=2, p1=',', t1='r')
 Purpose  : split string
 Author   : Adam
 Uses     : l1=split nums, p1=split string, t1=right-->left

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/01/25  Adam             Create
 1.1        2021/06/24  Adam             rewrite
-----------------------------------------------------------------------------'''
def usplit(s1, l1=1, p1=',', t1=''):

    l_return = []
    s_tmp = ''

    if t1:
        i = 0
        l2 = len(s1.split(p1)) - l1
        for o in s1.split(p1):
            if i < l2:
                s_tmp = s_tmp + p1 + o
                i = i + 1
                continue
            if not l_return:
                l_return.append(s_tmp)
            l_return.append(o)
    else:
        l_return = s1.split(p1, l1)

    return l_return

# uformat




'''-----------------------------------------------------------------------------
 Name     : valconv(l_dat, type=None)
 Purpose  : Convert special strings and dates in tuple or list, like : 0000-00-00 in date
 Author   : Adam
 Uses     : type=None, default is to process 2 level, CODE=gbk

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/04/20  Adam             Create
 1.1        2021/04/21  Adam             M01: Add character set conversion

-----------------------------------------------------------------------------'''
def valconv(l_dat, type=None, CODE='gbk'):
    t_dat = ()
    if not l_dat:
        return t_dat

    l_tmp1 = []
    if type:
        try:
            l_tmp1.clear()
            for rs1 in l_dat:
                if isinstance(rs1, str):
                    rs1 = rs1.encode(CODE, 'ignore').decode(CODE)     # M01: Add character set conversion

                if '0000-00-00 ' in str(rs1):
                    l_tmp1.append(None)
                else:
                    l_tmp1.append(rs1)
            t_dat = tuple(l_tmp1)
        except Exception as e:
            t_dat = l_dat
    else:
        l_tmp2 = []
        for rs2 in l_dat:
            try:
                l_tmp1.clear()
                for rs1 in rs2:
                    if isinstance(rs1, str):
                        rs1 = rs1.encode(CODE, 'ignore').decode(CODE) # M01: Add character set conversion
                    if '0000-00-00 ' in str(rs1):
                        l_tmp1.append(None)
                    else:
                        l_tmp1.append(rs1)
            except Exception as e:
                return valconv(l_dat, 1)
            l_tmp2.append(tuple(l_tmp1))
        t_dat = l_tmp2

    return t_dat
# END : valconv




'''-----------------------------------------------------------------------------
 Name     : ure(s1, c_s="'", s_t=" ")
 Purpose  : char replace
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2021/05/12  Adam             Create

-----------------------------------------------------------------------------'''
def ure(s1, c_s="'", s_t=" "):

    s_return = ''
    if s1:
        s_return = re.sub(r'[%s]'%c_s, s_t, s1)

    return s_return

# uformat


