#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import csv
import sqlite3

gci_map = {
        'B13 GCI 1': ['01', '02', '03', '04'],
        'B13 GCI 2': ['01', '02', '03', '04'],
        'B4 GCI 1': ['0C', '16', '20', '2A'],
        'B4 GCI 2': ['0C', '16', '20', '2A'],
        'B4 GCI 3': ['0D', '17', '21', '2B'],
        'B4 GCI 4': ['0D', '17', '21', '2B'],
        'B2 GCI 1': ['0E', '18', '22', '2C'],
        'B2 GCI 2': ['0E', '18', '22', '2C'],
        }

pci_map = {
        'PCI 1': ['B13 GCI 1', 'B4 GCI 1', 'B4 GCI 3', 'B2 GCI 1'],
        'PCI 2': ['B13 GCI 2', 'B4 GCI 2', 'B4 GCI 4', 'B2 GCI 2'],
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
            'Verizon',
            '311480',
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
        for pci_col in pci_map:
            pcis = row[cols[pci_col]]
            if pcis and pcis != '?':
                for gci_col in pci_map[pci_col]:
                    gci = row[cols[gci_col]]
                    if gci:
                        for idx, pci in enumerate(pcis.split(',')):
                            pci = pci.strip()
                            if pci != '?' and pci.isdigit():
                                insert(row[cols['TAC']],
                                        gci[:6] + gci_map[gci_col][idx],
                                        pci,
                                        row[cols['LAT']],
                                        row[cols['LONG']],
                                        name,
                                        confirmed)

conn.commit()
conn.close()
