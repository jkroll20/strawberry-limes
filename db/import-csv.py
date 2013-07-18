#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys
import re
import csv
import MySQLdb
import MySQLdb.cursors 

dbname= "rendertests";
tblname= "limes";


def single_degminsec2n(deg, min, sec, NW):
    v= float(re.sub(",", ".", str(sec)))/3600.0 + float(re.sub(",", ".", str(min)))/60.0 + float(str(deg))
    if not NW.strip() in ('N', 'O', 'E'):
        v= -v
    return v

def deg_min_sec_2_n(s):
    if s=='': return False
    try:
        #m= re.match(r"""([^°]*)° ([^']*)' ([^"]*)" (.), ([^°]*)° ([^']*)' ([^"]*)" (.)""", s)
        olds= s
        s= re.sub(r"[^0-9., \tNSWOE]", "#", str(s))
        m= re.match(r"""([^#]*)[# ]+([^#]*)[# ]+([^#]*)[# ]+(.),([^#]*)[# ]+([^#]*)[# ]+([^#]*)[# ]+(.)""", s)
        #print m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8)
        lat= single_degminsec2n(m.group(1), m.group(2), m.group(3), m.group(4))
        lon= single_degminsec2n(m.group(5), m.group(6), m.group(7), m.group(8))
        return (lat, lon)
    except Exception as e:
        #print("couldn't parse '%s' (was '%s'): %s" % (s, olds, str(e)))
        return False

def looks_like_int(s):
    try:
        if str(int(s)) == s.strip(): return True
        return False
    except:
        return False

def textfield_to_int(s):
    try:
        return int(s)
    except:
        return -10000

def preptext4db(s):
    return re.sub('\n', '<br>', s)

def process_row(row, cursor):
    # numbering removed
    #try:   # remove numbering
    #     if str(int(row[0]))==row[0].strip(): # some values use ',' as a thousands separator, some use '.'
    #    row= row[1:]
    #except:
    #    pass

    #OLD:
    #0      1               2               3               4               5           6               7           8           9       10      11              12+
    #LEMMA  LIMESABSCHNITT  BEGINN MÖGLICH  BEGINN SICHER   ENDE MÖGLICH    ENDE SICHER ZEITRAUM TEXT   KASTELLTYP  KOORDINATEN PROVINZ PROJEKT BEARBEITER / OK WILDWUCHS

    #NEW:
    #0      1               2               3               4               5               6           7               8           9           10      11      12              12+
    #LEMMA  ALTERNATIVNAME  LIMESABSCHNITT  BEGINN MÖGLICH  BEGINN SICHER   ENDE MÖGLICH    ENDE SICHER ZEITRAUM TEXT   KASTELLTYP  KOORDINATEN PROVINZ PROJEKT BEARBEITER / OK WILDWUCHS

    # check for valid geocoords
    coords= deg_min_sec_2_n(row[9])
    if not coords: 
        #print("couldn't parse geocoords '%s'" % row[9])
        return False

    #print("parsed geocoords '%s'" % row[9])
    
    if(textfield_to_int(row[3])==-10000): row[3]= row[4]
    if(textfield_to_int(row[5])==-10000): row[5]= row[6]

    # check for at least "possible begin" and "definite end"
    if not (looks_like_int(row[3]) and looks_like_int(row[6])):
        # print("years missing '%s' '%s'" % (row[3], row[6]))
        return False

    #if(textfield_to_int(row[4])==-10000): row[4]= 10000
    
    cursor.execute("SELECT * FROM limes WHERE lemma = %s AND beginnmoeglich = %s AND endemoeglich = %s AND beginnsicher = %s AND endesicher = %s AND kastelltyp = %s", \
        (preptext4db(row[0]), preptext4db(row[3]), preptext4db(row[5]), preptext4db(row[4]), preptext4db(row[6]), preptext4db(row[8])))
    result= cursor.fetchone()
    if result!=None:
        sys.stdout.write("duplicate: \n\t")
        for k in ('lemma', 'limesabschnitt', 'beginnmoeglich', 'endemoeglich', 'beginnsicher', 'endesicher', 'kastelltyp'):
            sys.stdout.write("%s: '%s'  " % (k, result[k])) 
        print ""

    try:
        cursor.execute("REPLACE INTO limes VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            #(re.sub(' ', '_', preptext4db(row[0])), 
            (preptext4db(row[0]),
            preptext4db(row[2]), textfield_to_int(row[3]), textfield_to_int(row[4]), textfield_to_int(row[5]), textfield_to_int(row[6]), \
            preptext4db(row[7]), \
            preptext4db(row[8]), 
            coords[0], 
            coords[1], 
            preptext4db(row[10]), 
            preptext4db(row[11]), 
            preptext4db(row[12])))
    except Exception as e:
        print "Exception:", str(e)
        print "Row:", row
        raise

    return True


def getDbCursor():
    conn= MySQLdb.connect( read_default_file=os.path.expanduser('~')+"/.my.cnf", use_unicode=False, cursorclass=MySQLdb.cursors.DictCursor )
    cursor= conn.cursor()
    
    try:
        cursor.execute("USE %s" % dbname)
    except MySQLdb.OperationalError as e:
        cursor.execute("CREATE DATABASE %s" % dbname)
        cursor.execute("USE %s" % dbname)
    
    try:
        cursor.execute("DESCRIBE %s" % tblname)
    except MySQLdb.ProgrammingError as e:
        #LEMMA  LIMESABSCHNITT  BEGINN MÖGLICH  BEGINN SICHER   ENDE MÖGLICH    ENDE SICHER ZEITRAUM TEXT   KASTELLTYP  KOORDINATEN PROVINZ PROJEKT BEARBEITER/OK
        cursor.execute("""CREATE TABLE %s (
                            lemma VARBINARY(255),                           \
                            limesabschnitt VARBINARY(255),                  \
                            beginnmoeglich INT, beginnsicher INT,           \
                            endemoeglich INT, endesicher INT,               \
                            zeitraumtext VARBINARY(255),                    \
                            kastelltyp VARBINARY(255),                      \
                            lat DOUBLE,                                     \
                            lon DOUBLE,                                     \
                            provinz VARBINARY(255),                         \
                            projekt VARBINARY(255),                         \
                            bearbeiternotiz VARBINARY(255),
                            KEY(beginnmoeglich),
                            KEY(beginnsicher),
                            KEY(endemoeglich),
                            KEY(endesicher),
                            KEY(kastelltyp)
                             )""" % tblname)
                            #UNIQUE KEY (lemma, beginnmoeglich,endemoeglich, beginnsicher,endesicher, kastelltyp),
    return conn, cursor

if __name__ == '__main__':
    inputfilename= 'gi.csv'
    if len(sys.argv)>1: inputfilename= sys.argv[1]
    valid_rows= 0
    conn, cursor= getDbCursor()
    cursor.execute('DELETE FROM %s' % tblname)
    with open(inputfilename) as csvfile:
        reader= csv.reader(csvfile)
        for row in reader:
            if process_row(row, cursor): valid_rows+= 1
    conn.commit()
    print("valid rows: %d" % valid_rows)
    cursor.execute('SELECT COUNT(*) FROM %s' % tblname)
    print('rows in DB: %s' % cursor.fetchone()['COUNT(*)'])
    cursor.execute('SELECT COUNT(DISTINCT(lemma)) FROM %s' % tblname)
    print('unique lemmas: %s' % cursor.fetchone()['COUNT(DISTINCT(lemma))'])
    cursor.close()
    conn.close()
    sys.exit(0)
