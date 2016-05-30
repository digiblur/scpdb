#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import csv
import sqlite3

gci_map = {
        'B4 1C': {
            'gci_col': 'B4 LTE GCI',
            'pci_col': 'B4 LTE PCI',
            'tac_col': 'TAC',
            'pattern': ['01', '02', '03', '04'],
            },
        'B2 1C': {
            'gci_col': 'B2 LTE GCI',
            'pci_col': 'B2 LTE PCI',
            'tac_col': 'TAC',
            'pattern': ['0B', '0C', '0D', '0E'],
            },
        'B12 1C': {
            'gci_col': 'B12 LTE GCI',
            'pci_col': 'B12 PCI',
            'tac_col': 'TAC',
            'pattern': ['15', '16', '17', '18'],
            },
        }

if len(sys.argv) < 3:
    print('Usage: ' + sys.argv[0] + ' csvfile dbfile')
    sys.exit(1)

conn = sqlite3.connect(sys.argv[2])
c = conn.cursor()

def insert(tac, gci, pci, lat, lon, name, conf):
    global c
    record = (
            name if conf else name + ' NEW',
            'T-Mobile',
            '310260',
            gci,
            pci,
            tac,
            '-60' if conf else '-140',
            lat,
            lon,
            gci,
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
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM sites_lte
            WHERE gci = ?)""", record)
    '''
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", record)
    '''

    if not c.rowcount:
        print('** DUPLICATE GCI**')
    print(tac, gci, pci, name)

with open(sys.argv[1], 'rb') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    cols = {x:ind for ind, x in enumerate(next(csvreader))}
    for row in csvreader:
        name = row[cols['Name']]
        lat = row[cols['LAT']]
        lon = row[cols['LONG']]
        confirmed = True
        if 'CONFIRM' in row[cols['Notes']].upper():
            confirmed = False
        for key in gci_map:
            gci = row[cols[ gci_map[key]['gci_col'] ]].strip()
            if gci and '?' not in gci and '999999' not in gci:
                inserted = False
                tac = row[cols[ gci_map[key]['tac_col'] ]]
                pcis = row[cols[ gci_map[key]['pci_col'] ]]
                pat = gci_map[key]['pattern']
                dec = int(gci, 16)
                for idx, pci in enumerate(pcis.split(',')):
                    pci = pci.strip()
                    if pci != '?' and pci.isdigit():
                        inserted = True
                        insert(tac,
                                '{:08X}'.format(dec + idx),
                                pci,
                                lat,
                                lon,
                                name,
                                confirmed)
                if not inserted:
                    insert(tac, gci, None, lat, lon, name, confirmed)

conn.commit()
conn.close()
