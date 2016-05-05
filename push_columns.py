import logging
import psycopg2
import re
import subprocess
import sys

conn = psycopg2.connect("dbname=sec10k user=postgres")
cur = conn.cursor()
exec_dir = 'raw_data/'

def update_columns():
    with open('date_filled.txt') as f:
        for line in f:
            file_num = re.findall('[0-9]*?(?=.txt|.html)', line)[0]
            date_filled = re.findall('(?<=\s)[0-9]{8}', line)[0]
            year = date_filled[0:4]
            month = date_filled[4:6]
            day = date_filled[6:8]
            full_date = day + '-' + month + '-' + year
            print file_number

            is_html = subprocess.call('grep -q \<PAGE\> ' + exec_dir + str(file_number) + '.txt', shell=True)
            if is_html == 0:
                html = False
            else:
                html = True

            cur.execute("SELECT * FROM sec10k WHERE file_number = (%s);", (207,)) # use file_num
            q = cur.fetchone()

            if q is None:
                #it's 10KA, insert line
                cur.execute("INSERT INTO sec10k (file_number, report_type, is_html) VALUES (%s, %s, %s),"
                            (file_num, '10KA', html))
                print 'azaz'

            else:
                cur.execute("UPDATE sec10k SET report_date = (%s), report_type = (%s), is_html =(%s) where file_number=(%s);",
                            (full_date, '10K', html, file_num))
            conn.commit()
            break

update_columns()

# cur.execute('INSERT INTO sec10k (file_number, mda_line_start, mda_line_end) VALUES (%s, %s, %s)',
#                     (j, res_s['line'], res_e['line']))