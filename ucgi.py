# -*- coding: utf-8 -*-
#
#
#  User-defined package
#
#
r"""
User-defined package
Support for URL/CGI v2.1
Copyright (c) 2018-2021 by Adam, All rights reserved.

This means that no one may use your work unless they obtain your permission.
This statement is not legally required, and failure to include it has no legal
significance. Since others may not use copyrighted works without the copyright
holder's permission, the statement is redundant.
"""

import sys
import os
'''-----------------------------------------------------------------------------
 Name     : ulog to url
 Purpose  : write log to URL
 Author   : Adam
 Uses     : urllib

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/04/16  Adam             Create
 1.2        2019/06/20  Adam             Add urllib.parse.quote
 2.0        2019/12/10  Adam             rewrite
 2.1        2020/05/21  Adam             M01:d_val format
-----------------------------------------------------------------------------'''
def ulog(dval):
    '''write log to URL
    {'data', 'type'}
    '''

    import ustr
    import urllib.request
    import urllib.parse

    #from urllib import request
    #from urllib import parse

    dval_default = {
        'int_p'     : 'http',
        'ip'        : '10.10.137.16, 10.10.137.41',
        'path'      : '/cgi-bin/logsm',
        'balance'   : 'yes',
        'rest'      : 'post',
        'uagent'    : 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }

    try:

        # variable INIT
        int_p    = dval.get('int_p',    dval_default['int_p'])
        ip       = dval.get('ip',       dval_default['ip'])
        path     = dval.get('path',     dval_default['path'])
        balance  = dval.get('balance',  dval_default['balance'])
        rest     = dval.get('rest',     dval_default['rest'])
        uagent   = dval.get('uagent',   dval_default['uagent'])

        # write data, M01:d_val format
        d_val    = {}
        d_val['type'] = dval.get('type',     'default')
        d_val['data'] = dval.get('data',     {})

        ip_l = ustr.ulist(ip)

        if not d_val:
            return 'data is NULL.'

        #http proxy
        #proxy  = dval.get('proxy',  '')
        #proxys  = dval.get('proxys',  '')
        #os.environ['http_proxy'] = proxy                   # 'http://127.0.0.1:8080'
        #os.environ['https_proxy'] = proxys                 # 'https://127.0.0.1:8080'

        # Web Host
        url_template = int_p + '://' + '%IP%' + '/' + path.strip('/')

        # param
        param = urllib.parse.urlencode(d_val).encode('utf-8')


        url = url_template.replace('%IP%', ip_l[0])
        if rest != 'get':
            # post
            headers = { 'User-Agent': uagent }
            request = urllib.request.Request(url, param, headers)
            rs = urllib.request.urlopen(request).read().decode('utf-8')

        else:
            # get
            val = urllib.parse.quote(str(value))
            s_url = url + '?' + param
            str_r = urllib.request.Request(s_url)           # send string
            rs = urllib.request.urlopen(str_r).read()

        return rs

    except Exception as e:
        return e

# ulog



'''-----------------------------------------------------------------------------
 Name     : uform
 Purpose  : Get Web Form Data
 Author   : Adam
 Uses     :

 Revisions:

 Ver        Date        Author           Description
 ---------  ----------  ---------------  --------------------------------------
 1.0        2019/05/26  Adam             Create
 2.0        2019/12/10  Adam             rewrite
 2.1        2021/07/04  Adam             rewrite

-----------------------------------------------------------------------------'''
#import sys
#import os
def uform(header = 'CGI', std_in = sys.stdin.read(int(os.environ.get("CONTENT_LENGTH", 0)))):
    '''Get Web Form Data
    {'header'}'''

    CGI_TYPE = os.environ.get("CONTENT_TYPE", 'NULL').lower()
    
    data = {}
    try:
        if std_in:
            dv2 = {}
            # json
            if 'json' in CGI_TYPE:
                import ustr
                dv1 = ustr.udict(std_in)
                for tmp in dv1:
                    dv2[tmp.lower()] = dv1[tmp]
            else:
                from urllib import parse
                dv1 = parse.parse_qs(std_in)
                for tmp in dv1:
                    dv2[tmp.lower()] = dv1[tmp][0]

            data[header] = dv2

            return data

        # form
        import cgi
        web_in = cgi.FieldStorage()
        d_form = {}
        for key in web_in:
            d_form[key.lower()] = web_in.getvalue(key)

        data[header] = d_form

        return data

    except Exception as e:
        print('error from ucgi-uform')
        print(e)
        return {}

# uform
