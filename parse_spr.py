#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import csv
import sqlite3

gci_map = {
        'B25 1C': {
            'gci_col': 'PCS LTE GCI',
            'pci_col': 'PCS LTE PCI',
            'tac_col': 'B25 TAC',
            'confirm': 'PCS LTE Confirmed',
            'pattern': ['01', '02', '03'],
            },
        'B25 2C': {
            'gci_col': 'PCS LTE GCI',
            'pci_col': 'PCS LTE PCI',
            'tac_col': 'B25 TAC',
            #'confirm': 'PCS 2C Confirmed',
            'confirm': 'PCS LTE Confirmed',
            'pattern': ['09', '0A', '0B'],
            },
        'B26 1C': {
            'gci_col': '800 LTE GCI',
            'pci_col': 'PCS LTE PCI',
            'tac_col': 'B26 TAC',
            'confirm': '800 LTE Confirmed',
            'pattern': ['19', '1A', '1B'],
            },
        'B41 1C': {
            'gci_col': '2.5 LTE GCI',
            'pci_col': '2.5 LTE PCI',
            'tac_col': 'B41 TAC',
            'confirm': '2.5 LTE Confirmed',
            'pattern': ['31', '32', '33'],
            },
        'B41 2C': {
            'gci_col': '2.5 LTE GCI',
            'pci_col': '2.5 LTE PCI',
            'tac_col': 'B41 TAC',
            #'confirm': '2.5 2C Confirmed',
            'confirm': '2.5 LTE Confirmed',
            'pattern': ['39', '3A', '3B'],
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
            'Sprint',
            '310120',
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

def process(tac, gci, pci, cnf, pat, lat, lon, name):
    pcis = [x.strip() for x in pci.split(',')]
    for idx, sid in enumerate(pat):
        _pci = pcis[idx] if idx < len(pcis) and pcis[idx].isdigit() else ''
        _gci = gci[:6] + sid
        print(tac, _gci, _pci, name)
        insert(tac,
                _gci,
                _pci,
                lat,
                lon,
                name,
                True if cnf else False)

with open(sys.argv[1], 'rb') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    cols = {x:ind for ind, x in enumerate(next(csvreader))}
    for row in csvreader:
        name = row[cols['Name']]
        lat = row[cols['LAT']]
        lon = row[cols['LONG']]
        for key in gci_map:
            gci = row[cols[ gci_map[key]['gci_col'] ]].strip()
            if gci and '?' not in gci:
                tac = row[cols[ gci_map[key]['tac_col'] ]]
                pci = row[cols[ gci_map[key]['pci_col'] ]]
                cnf = row[cols[ gci_map[key]['confirm'] ]]
                pat = gci_map[key]['pattern']
                #print(tac, gci, pci, cnf, pat, lat, lon, name)
                process(tac, gci, pci, cnf, pat, lat, lon, name)

conn.commit()
conn.close()
