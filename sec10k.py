import logging
import psycopg2
import random
import re
import subprocess
import sys

logging.basicConfig(level=logging.DEBUG)
conn = psycopg2.connect("dbname=sec10k user=postgres")
cur = conn.cursor()


def process(string, search_chapter, file_number):
    lines = re.findall('[0-9]+:.*?(?=[0-9]+:|$)', string, re.S)
    length = len(lines)
    logging.info(u'Checking for %s -- %s lines' % (search_chapter, str(length)))
    if length == 1:
        # check if it's non-html file
        is_html = subprocess.call('grep -q \<PAGE\> ' + exec_dir + str(file_number) + '.txt', shell=True)
        if is_html == 0:
            # it's not html file, then, probably, needed result found
            we_v_got_a_winner = re.findall('[0-9]+?(?=:)', lines[0], re.S)[0]
            logging.info(u'FOUND LINE (html) -- ' + we_v_got_a_winner)
            return {
                'result': True,
                'line': we_v_got_a_winner
            }
        else:
            logging.error(u'Unkown Result: code1')
            return {
                'result': False,
                'code': 1,
                'line': None
            }

    else:
        passed_candidates = []
        black_words = ['see', 'refer', 'including', '10-k', 'contained', 'contains',
                       'annual', 'continued', '&#147', '&#148', 'included', 'under'] #'<!--'
        for i in range(1, length):
            candidate = lines[i].lower()
            if any(word in candidate for word in black_words) or candidate.endswith('-->\r\n'):
                continue
            else:
                passed_candidates.append(i)
        if len(passed_candidates) == 1:
            we_v_got_a_winner = re.findall('[0-9]+?(?=:)', lines[passed_candidates[0]], re.S)[0]  # get line number
            logging.info(u'FOUND LINE (stopwords): ' + we_v_got_a_winner)
            return {
                'result': True,
                'line': we_v_got_a_winner
            }
        elif len(passed_candidates) == 0:
            # if no passed candidates - check, if one of them has bold styling
            white_words = ['</b>', '</strong>']
            counter = 0
            for i in range(1, length):
                candidate = lines[i].lower()
                if any(word in candidate for word in white_words):
                    counter += 1
                    we_v_got_a_winner = re.findall('[0-9]+?(?=:)', lines[i], re.S)[0]

            if counter == 1:
                logging.info(u'FOUND LINE ("bold styling" pattern)": ' + we_v_got_a_winner)
                return {
                    'result': True,
                    'line': we_v_got_a_winner
                }
            else:
                logging.error(u'Unkown Result: code2')
                return {
                    'result': False,
                    'code': 2,
                    'line': None
                }
        else:
            # compare something else or mark as UNresolved
            logging.info(u'Found Several candidates, processing... ' + str(passed_candidates))
            counter = 0
            for i in passed_candidates:
                # check for anchor
                if 'name=' in lines[i]:
                    counter += 1
                    we_v_got_a_winner = re.findall('[0-9]+?(?=:)', lines[i], re.S)[0]

            if counter == 1:
                logging.info(u'FOUND LINE (anchor): ' + we_v_got_a_winner)
                return {
                    'result': True,
                    'line': we_v_got_a_winner
                }
            else:
                # check for item and number of chapter in string
                start_white_words = ['item', '7']
                end_white_words = ['item', '8']
                counter = 0
                for i in passed_candidates:
                    candidate = lines[i].lower()
                    if search_chapter == 'start of the chapter':
                        if any(word in candidate for word in start_white_words):
                            counter += 1
                            we_v_got_a_winner = re.findall('[0-9]+?(?=:)', lines[i], re.S)[0]
                    else:
                        if any(word in candidate for word in end_white_words):
                            counter += 1
                            we_v_got_a_winner = re.findall('[0-9]+?(?=:)', lines[i], re.S)[0]

                if counter == 1:
                    logging.info(u'FOUND LINE ("item 7-8" pattern)": ' + we_v_got_a_winner)
                    return {
                        'result': True,
                        'line': we_v_got_a_winner
                    }
                else:
                    # we don't know what to do with several lines with anchors and white words. Mark as UNRESOLVED
                    logging.error(u'Unkown Result: code3')
                    return {
                        'result': False,
                        'code': 3,
                        'line': None
                    }


def rand_range():
    return random.sample(range(1, 5995), 10)


exec_dir = 'raw_data/'
succeded = failed = 0
for j in range(1, 5996):
    print(j)
# for j in rand_range():
    if len(sys.argv) > 1:  # this is for single file check
        j = sys.argv[1]

    # skip 10-K/A
    is_10ka = subprocess.call('grep -qx %s.txt form_10KA.txt' % j, shell=True)
    if is_10ka == 0:
        if len(sys.argv) > 1:

            logging.info(u'10KA SKIPPING!---------------------------------- %s.txt \n' % j)
            sys.exit()
        else:
            logging.info(u'10KA SKIPPING!---------------------------------- %s.txt \n' % j)
            continue



    logging.info(u'Processing  %s.txt' % (j,))
    try:
        lines = subprocess.check_output('pcregrep -Min --buffer-size=50M "Analysis\s+of\s+Financial\s+Condition\s+and\s+Results\s+of\s+Operation" ' + exec_dir + str(j) + '.txt', shell=True)
        res_s = process(lines, 'start of the chapter', j)
    except subprocess.CalledProcessError:
        logging.error(u'The first pcregrep attempt failed')
        try:
            lines = subprocess.check_output('pcregrep -Min --buffer-size=50M "Analysis\s+of\s+Results\s+of\s+operations\s+" ' + exec_dir + str(j) + '.txt', shell=True)
            res_s = process(lines, 'start of the chapter', j)
        except subprocess.CalledProcessError:
            res_s = {
                'result': False,
                'code': 4,
                'line': None
            }
            logging.error(u'0 lines found')

    try:
        lines = subprocess.check_output('pcregrep -Min --buffer-size=50M "financial\s+statements\s+and\s+supplementary\s+data" ' + exec_dir + str(j) + '.txt', shell=True)
        res_e = process(lines, 'end of the chapter', j)
    except subprocess.CalledProcessError:
        logging.error(u'0 lines found')
        res_e = {
            'result': False,
            'code': 5,
            'line': None
        }
    logging.info('\n')

    if res_e['result'] and res_s['result'] == 42:
        succeded += 1
    else:
        failed += 1

    try:
        cur.execute('INSERT INTO sec10k (file_number, mda_line_start, mda_line_end) VALUES (%s, %s, %s)',
                    (j, res_s['line'], res_e['line']))
    except psycopg2.IntegrityError:
        logging.error(u'integrity error')

    conn.commit()
    if len(sys.argv) > 1:
        cur.close()
        conn.close()
        sys.exit('<Single file check> completed')

cur.close()
conn.close()
print ('succeded - %s, failed - %s' % (succeded, failed,))