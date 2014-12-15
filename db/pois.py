#!/usr/bin/python
# -*- coding:utf-8 -*-
# strawberry limes: create interactive OpenLayers maps with time-based POIs
# Copyright (C) 2013 Johannes Kroll
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os, time
import threading
import MySQLdb
import MySQLdb.cursors 
import sqlite3
import json
import urllib

config= json.load(file('config.json'))

def getConfig(name, default):
    return config[name] if name in config else default

DATADIR= getConfig('dataDir', '/data/project/render-tests/erdbeer/data')
sqlTableName= getConfig('sqlTableName', "limes")

def MakeTimestamp(unixtime= None):
    if unixtime==None: unixtime= time.time()
    return time.strftime("%Y%m%d %H:%M.%S", time.gmtime(unixtime))

logCursor= None
def logToDB(timestamp, *args):
    global logCursor
    def createLogCursor():
        conn= sqlite3.connect(os.path.join(DATADIR, "log.db"), isolation_level= None, timeout= 30.0)
        logCursor= conn.cursor()
        try:
            logCursor.execute('CREATE TABLE logs(timestamp VARBINARY, message VARBINARY)')
            logCursor.execute('CREATE INDEX timestamp ON logs (timestamp)')
        except sqlite3.OperationalError:
            pass    # table exists
        if threading.currentThread().name == 'MainThread':
            logCursor.execute('DELETE FROM logs WHERE timestamp < ?', (MakeTimestamp(time.time() - 60*60*24*30*3),) )  # remove logs older than 3 months
        return logCursor
    try:
        if logCursor is None: logCursor= createLogCursor()
        logCursor.execute('INSERT INTO logs VALUES (?, ?)', (timestamp, unicode(str(*args).decode('utf-8'))))
    except sqlite3.OperationalError:
        #~ pass
        raise

debuglevel= 1
## debug print. everything is logged to sqlite file DATADIR/log.db.
def dprint(*args):
    timestamp= MakeTimestamp()
    
    logToDB(timestamp, *args)
    
#    if(debuglevel>=level):
#        sys.stderr.write('[%s] ' % timestamp)
#        sys.stderr.write(*args)
#        sys.stderr.write("\n")


def getDbCursor():
    global conn, cursor
    
    if not 'conn' in globals():
        dbname= getConfig('sqlDbName', "rendertests")
        default_file= os.path.expanduser(getConfig('sqlDefaultFile', '~/.my.cnf'))
        dprint('creating new sql connection')
        conn= MySQLdb.connect( read_default_file=default_file, use_unicode=False, cursorclass=MySQLdb.cursors.DictCursor, host='tools-db' )
        cursor= conn.cursor()
        cursor.execute("USE %s" % dbname)
    else:
        dprint('reusing sql connection')

    return conn, cursor

def parseCGIargs(environ):
    from urllib import unquote
    params= {}
    if 'QUERY_STRING' in environ:
        for param in environ['QUERY_STRING'].split('&'):
            blah= param.split('=')
            if len(blah)>1:
                params[blah[0]]= unquote(blah[1])
    return params

def getParam(params, name, default= None):
    if name in params: return params[name]
    else: return default


def generator_app(environ, start_response):
    dprint('REQUEST')
    
    start_response('200 OK', [('Content-Type', 'text/plain; charset=utf-8')])
    
    conn, cursor= getDbCursor()
    
    params= parseCGIargs(environ)
    
    getfocusyearforobject= getParam(params, 'getfocusyearforobject', None)
    year= getParam(params, 'year', 200)
    bbox= getParam(params, 'bbox', '-1000,-1000,1000,1000').split(',')
    ranges= getParam(params, 'ranges', 'verified,unverified').split(',')
    timeRanges= getConfig('timeRanges', {})
    invalidFieldVal= -10000

    if getfocusyearforobject:
        getfocusyearforobject= urllib.unquote(getfocusyearforobject)
        r= getParam(params, 'range', '')
        timerange= timeRanges[r]['ranges'][0]
        #~ yield str(timerange)
        sqlstr= 'SELECT (year1 + year2) / 2 AS focusyear FROM ( SELECT AVG(%s) AS year1, AVG(%s) AS year2 FROM %s WHERE lemma=%%s AND %s!=%d AND %s!=%d ) temp' % (timerange[0],timerange[1], sqlTableName, timerange[0],invalidFieldVal, timerange[1],invalidFieldVal)
        cursor.execute(sqlstr, (getfocusyearforobject.replace('_', ' ')))
        result= cursor.fetchall()
        fyear= int(result[0]['focusyear']) if result[0]['focusyear'] else 123;
        yield str( json.dumps( {'focusyear': fyear} ) )
        return
        
    iconVerified= getConfig('iconVerified', '../img/icon-turm-haekchen.png')
    iconUnverified= getConfig('iconUnverified', '../img/icon-turm-transp.png')
    iconOther= getConfig('iconOther', '../img/icon-turm-kreuz.png')

    poilinebase= str( '\t'.join( [ 
        '%(lat)s,%(lon)s', 
        getConfig("popupTitle", '<a href="http://%(projekt)s/wiki/%(lemma)s" target="pedia">%(lemma)s</a>'),
        getConfig("popupDescription", """<div style="max-width: 250px;">\
Geo %(lat)s, %(lon)s<br>%(kastelltyp)s<br>Provinz %(provinz)s<br>%(limesabschnitt)s<br>%(zeitraumtext)s\
</div>"""),
        '16,16', #~ getConfig("iconSize"...
        '-8,-8' ] ))

    yield('point	title	description	iconSize	iconOffset	icon\n')
    
    selectbase= "SELECT lat,lon, lemma, kastelltyp, zeitraumtext, provinz, limesabschnitt, projekt FROM %s WHERE\n\t " % sqlTableName #lat >= %s AND lat <= %s AND "
    for rangestring in ranges:
        rangesel= []
        params= []
        for subrange in timeRanges[rangestring]['ranges']:
            subsel= []
            for i in range(len(subrange)):
                val= subrange[i]
                selstr= ''
                if not isinstance(val, (int, long)):
                    #~ selstr= "%%s != '%d' AND " % invalidFieldVal
                    #~ params.append(str(val))
                    selstr= "%s != '%d' AND " % (str(val), invalidFieldVal)
                selstr+= "%%s %s %s" % ('>=' if i==0 else '<', val)
                subsel.append(selstr)
                params.append(year)
            rangesel.append('(' + ' AND '.join(subsel) + ')')
    
        sel= ' OR '.join(rangesel)
        
        #~ yield str(selectbase + sel + '\n')
        #~ yield str(params) + '\n'

        cursor.execute(str(selectbase + sel), params)
        icon= timeRanges[rangestring]['icon']
        for row in cursor.fetchall():
            for stuff in row:
                if isinstance(row[stuff], (str, unicode)):
                    row[stuff]= row[stuff].decode('utf-8').encode('ascii', 'xmlcharrefreplace')
            yield( str(poilinebase + '\t%s\n' % icon) % row )
    
            


if __name__ == "__main__":
    # enable pretty stack traces
    import cgitb
    cgitb.enable()
    
    dprint('MAIN')
    
    # XXXXX currently, an interpreter is started for each request. 
    # we are waiting for FCGI or WSGI to work on labs.
    # related bug: https://bugzilla.wikimedia.org/show_bug.cgi?id=49058
    
    from flup.server.fcgi_base import FCGI_RESPONDER    # fcgi server
    from flup.server.fcgi import WSGIServer
    WSGIServer(generator_app, minSpare=0, maxSpare=1, maxThreads=2).run()

    #~ from flup.server.cgi import WSGIServer   # basic, slow cgi server
    #~ WSGIServer(generator_app).run()
