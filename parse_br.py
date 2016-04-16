#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import csv
import re
import sqlite3

pattern = r'([a-fA-F0-9xX]{8})'
"""
with open('att-br.csv', 'rb') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    col = {x:ind for ind, x in enumerate(next(csvreader))}
    for row in csvreader:
        matches = set(int(x[:6], 16) for x in re.findall(
            pattern, row[col['Notes']]))
        if len(matches):
            print(matches, row[col['Notes']])
"""



gci_map = {
        'B17 LTE PCI': ['0F', '10', '11', '12'],
        'B4 LTE PCI': ['16', '17', '18', '19'],
        'B2 LTE PCI': ['08', '09', '0A', '0B'],
        #'B4 PCI 2': ['32', '33', '34', '35'],
        'B5 PCI': ['01', '02', '03', '04'],
        }

if len(sys.argv) < 3:
    print('Usage: ' + sys.argv[0] + ' csvfile dbfile')
    sys.exit(1)

conn = sqlite3.connect(sys.argv[2])
c = conn.cursor()

def insert(tac, gci, pci, lat, lon, name, conf):
    global c
    record = (
            name,
            'AT&T',
            '310410',
            gci,
            pci,
            tac,
            '-60' if conf else '-140',
            lat,
            lon,
            )
    c.execute("""INSERT INTO sites_lte
            (user_note,
            provider,
            plmn,
            gci,
            pci,
            tac,
            strongest_rsrp,
            strongest_latitude,
            strongest_longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", record)

with open(sys.argv[1], 'rb') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    cols = {x:ind for ind, x in enumerate(next(csvreader))}
    for row in csvreader:
        name = row[cols['Name']]
        if name.endswith('*'):
            name = name[:-1]
        confirmed = True
        if 'CONFIRM' in row[cols['Notes']]:
            confirmed = False
            name = name + ' NEW'
        processed = list()
        for gci_col in ['B17 LTE GCI', 'B4 LTE GCI', 'B2 LTE GCI', 'B5 LTE GCI', 'Notes']:
            if gci_col == 'Notes':
                matches = set(int(x[:6], 16) for x in re.findall(
                    pattern, row[cols['Notes']]))
                if len(matches):
                    # For now just use the first match
                    gci = '{:06X}xx'.format(matches.pop())
            else:
                gci = row[cols[gci_col]]
            if gci:
                if gci in processed:
                    continue
                processed.append(gci)
                for pci_col in gci_map:
                    pcis = row[cols[pci_col]]
                    if pcis:
                        for idx, pci in enumerate(pcis.split(',')):
                            pci = pci.strip()
                            if pci != '?' and pci.isdigit():
                                insert(row[cols['TAC']],
                                        gci[:6] + gci_map[pci_col][idx],
                                        pci,
                                        row[cols['LAT']],
                                        row[cols['LONG']],
                                        name,
                                        confirmed)



conn.commit()
conn.close()