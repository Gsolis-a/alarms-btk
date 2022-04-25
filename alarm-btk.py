

#!/usr/bin/python3

#

# @ rbl/2021

#

import psycopg2

import psycopg2.extras

from collections import defaultdict

import redis

import requests

import json

import sys

import os

# minutes to scan

ALARM_SCAN_MINUTES = 4

# minimum number of total txs to consider alarming

ALARM_MINIMUM_TOTAL = 50

# alarm if the failure percentage is (strictly) more than this

ALARM_THRESHOLD = 0.50

def get_top_alarm(dd, title):  # sourcery skip: comprehension-to-generator, simplify-len-comparison, use-fstring-for-formatting

    edd = {k: dd[k] for k in dd if k != 'OK'}

    total = sum([dd[x] for x in dd])

    if len(edd) > 0 and total >= ALARM_MINIMUM_TOTAL and dd['OK'] < (1.0 - ALARM_THRESHOLD) * total:

        errors = sum([edd[x] for x in edd])

        top_error = sorted(edd.items(), key=lambda x: x[1])[-1][0]

        return 'Alarma {} {}/{} err/tot ({}%), top error ({} err) "{}"'.format(title, errors, total, round(100.0*errors/total, 1), dd[top_error], top_error)

    else:

        return None

def slack_notify(chan, msg):

    url = 'https://u9f3q68aw8.execute-api.sa-east-1.amazonaws.com/v1/slack'

    body = json.dumps({'product':os.path.basename(__file__), 'severity':'ERROR', 'message':msg, 'channel':chan})

    requests.post(url, data=body, headers={'Content-Type':'application/json'})
    
def back_off_alarm(alarm):

    r = redis.Redis(
        host='hostname',
        port='port', 
        password='password')

    if alarm_iter_value := r.get('iter_counter'):
        
        r.set('iter_counter', int(alarm_iter_value + 1))
        
        trigger_alarmg_iter_values = {1, 2 ,4 ,8 ,16, 32, 64}

        if alarm_iter_value in trigger_alarmg_iter_values:
            pass
            
            # if alarm_iter_value > max(trigger_alarmg_iter_values):
                
                
            # else:
            #     return True
            
            # return False
        
    else:
        r.set('iter_counter', 1)

def main():

    global ALARM_SCAN_MINUTES, ALARM_MINIMUM_TOTAL, ALARM_THRESHOLD

    try:

        to_slack = int(sys.argv[1]) != 0

    except IndexError:

        to_slack = True

    if len(sys.argv) >= 3:

        ALARM_SCAN_MINUTES = int(sys.argv[2])

    if len(sys.argv) >= 4:

        ALARM_MINIMUM_TOTAL = int(sys.argv[3])

    if len(sys.argv) >= 5:

        ALARM_THRESHOLD = float(sys.argv[4])

    cards_all = defaultdict(int)

    cards_per_bank = defaultdict(lambda: defaultdict(int))

    cards_per_brand = defaultdict(lambda: defaultdict(int))

    with psycopg2.connect('host=10.26.0.27 port=5432 user=redelcom dbname=redelcom') as con:

        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT case WHEN msg_visor ILIKE '%%re-intente%%' OR msg_visor ILIKE '%%comunica%%' THEN msg_visor ELSE 'OK' END AS msg, brand, bank, count(1) FROM (SELECT trim(tc.msg_visor) AS msg_visor, bb.brand, bb.bank FROM transacciones_credito tc LEFT OUTER JOIN binbase bb ON (bb.bin = left(tc.pan, 6)) WHERE hora_ini_switch > now()-interval'%s minutes' AND pan <> '' AND estado_tx IN ('C', 'E')) foo GROUP BY msg_visor, brand, bank", (ALARM_SCAN_MINUTES,))

        for row in cur.fetchall():

            cards_all[row['msg']] += row['count']

            cards_per_bank[row['bank']][row['msg']] += row['count']

            cards_per_brand[row['brand']][row['msg']] += row['count']

    errors = []

    es = get_top_alarm(cards_all, 'Total')

    if es is not None:

        errors.append(es)

    for brand in cards_per_brand:

        es = get_top_alarm(cards_per_brand[brand], 'Marca: ' + (brand or ''))

        if es is not None:

            errors.append(es)

    for bank in cards_per_bank:

        es = get_top_alarm(cards_per_bank[bank], 'Banco: ' + (bank or ''))

        if es is not None:

            errors.append(es)

    if errors != []:

        final_error = 'Ultimos {} minutos:\n'.format(ALARM_SCAN_MINUTES) + '\n'.join(errors).strip()

        if to_slack:

            slack_notify('#alarmas-transbank', final_error)

        print(final_error)

if __name__ == '__main__':

    main()

# my_alarms = 'tbk-alarm:60:total' + ['tbk-alarm:60:brand:'+x for x in cards_per_brand] + ['tbk-alarm:60:bank:'+x for x in cards_per_bank]