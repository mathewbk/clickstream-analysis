# Databricks notebook source
#!/usr/local/bin/python

import pyodbc
import os
import sys
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
             

email_list = sys.argv[1]
td_staging_table = sys.argv[2]
outcome_column = sys.argv[3]
failed_ind = sys.argv[4]
stg_schema = sys.argv[5]
dsn = sys.argv[6]
db = sys.argv[7]
db = db.upper()

try:
    pyodbc.pooling = False
    cnxn = pyodbc.connect('DSN=%s' %dsn, ansi=True)
except:
    print 'ERROR: Could not connect to the database using DSN \'%s\'' %dsn
    print 'Verify there is an entry for this in odbc.ini'
    raise

cursor = cnxn.cursor()
row_number=0

## query the staging table for assertions.

sql = 'select process_date, source, platform, event_description, validation_rule, actual_outcome, validation_outcome '
sql = sql + 'from ' + stg_schema + '.' + td_staging_table + ' order by process_date,source,platform,event_code'
try:
    cursor.execute(sql)
except:
    print 'ERROR: Could not execute the following DML statement against database:\n'
    print sql+'\n'
    raise

try:
    message = MIMEMultipart('alternative')
    message.add_header('Subject', db + ' Attribution source table audits')
    html = '<html>Attribution source table audits:<br><br><table border="1"  cellpadding="5" style="border-collapse:collapse">'
    html = html + '<tr><td><b>#</b></td><td><b>EVENT DATE</b></td><td><b>PLATFORM</b></td><td><b>AUDIT</b></td><td><b>OUTCOME</b></td><td><b>STATUS</b></td></tr>'
    rows = cursor.fetchall()
    for row in rows:
       row_number = row_number + 1
       html = html + '<tr>'
       html = html + '<td>' + str(row_number) + '</td>'
       html = html + '<td>' + str(row.process_date) + '</td>'
       html = html + '<td>' + str(row.platform) + '</td>'
       html = html + '<td>' + row.validation_rule + '</td>'
       html = html + '<td>' + row.actual_outcome + '</td>'
       if row.validation_outcome == 'FAILED': 
           html = html + '<td><font color = "red"><b>' + row.validation_outcome + '</b></font></td>'
       else:
           html = html + '<td><font color = "green"><b>' + row.validation_outcome + '</b></font></td>'
           html = html + '</tr>'
    html = html + '</table></html>'
    message.attach(MIMEText('' + html + '',
                         'html'))


    sendmail = os.popen('sendmail -t', 'w')
    sendmail.write("To: %s\n" % email_list)
    sendmail.write(message.as_string())
    print 'Successfully sent out email notifications.\n' 
except:
    print 'ERROR: Could not send email notifications via sendmail.\n'
    raise

sys.exit(0)
