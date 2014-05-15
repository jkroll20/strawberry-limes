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

import os
import cgi
import cgitb
import time
import subprocess

def MakeTimestamp(unixtime= None):
    if unixtime==None: unixtime= time.time()
    return time.strftime("%Y%m%d-%H:%M.%S", time.gmtime(unixtime))


def generator_app(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
    
    yield "<html><body>\n"

    #~ if 'CONTENT_LENGTH' in environ and int(environ['CONTENT_LENGTH'])!=0:
        #~ request_body= environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
    
    form= cgi.FieldStorage()
    
    if not 'csv' in form:
        yield "Fehler: Keine Datei zum Hochladen angegeben.</body></html>"
        return
    
    csv= form['csv']
    #~ for i in ('filename', 'file', 'value'):
        #~ if i in csv.__dict__: yield '%s: %s\n' % (i, csv.__dict__[i])
    
    if not csv.filename.endswith('.csv'):
        yield "Fehler: Dateiname muss auf .csv enden.</body></html>"
        return
    
    outfilename= 'files/%s.csv' % MakeTimestamp()
    
    yield 'Original-Dateiname: %s<br>\n' % csv.filename
    yield 'Ausgabedatei/Backup: <a href="%s">%s</a><br>' % (outfilename, outfilename)
    
    outputfile= open(outfilename, 'w')
    
    for line in csv.file:
        outputfile.write(line)
    outputfile.close()
    
    yield '<pre>\n'
    try:
        cmd= '../db/import-csv.py %s' % outfilename
        output= subprocess.check_output( cmd, shell=True, stderr=subprocess.STDOUT )
        yield output
        yield '\n\nImport OK.\n'
    except subprocess.CalledProcessError as ex:
        yield 'Fehler: \n'
        yield str(ex) + '\n'
        yield 'Ausgabe des Skripts war:\n'
        yield str(ex.output)
        yield '\n\nImportieren fehlgeschlagen.\n'
    yield '</pre>\n'
        
    yield '</body></html>\n'

if __name__ == '__main__':
    # enable pretty stack traces
    cgitb.enable()

    from flup.server.cgi import WSGIServer
    WSGIServer(generator_app).run()

