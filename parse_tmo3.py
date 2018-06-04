#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import csv
import sqlite3

gci_map = {
        'L2100 PCI': ['01', '02', '03', '04', '05', '06', '07', '08', '09'],
        'L1900 PCI':  ['0B', '0C', '0D', '0E', '0F', '10'],
        'L700 PCI': ['15', '16', '17', '18', '19', '1A'],
        '2c L2100 PCI': ['65', '66', '67', '68', '69', '6A'],
        'B66 PCI':  ['1F', '20', '21', '22', '23', '24'],
        'L600 PCI':  ['29', '2A', '2B', '2C', '2D', '2E'],
        }

# CREATE TABLE sites_lte (_id INTEGER PRIMARY KEY, first_time NUMERIC, first_time_offset NUMERIC, last_time NUMERIC, last_time_offset NUMERIC, last_device_latitude NUMERIC, last_device_longitude NUMERIC, last_device_loc_accuracy NUMERIC, user_note TEXT, provider TEXT, plmn NUMERIC, gci TEXT, pci NUMERIC, tac NUMERIC, dl_chan NUMERIC, strongest_rsrp NUMERIC, strongest_latitude NUMERIC, strongest_longitude NUMERIC)

if len(sys.argv) < 3:
    print('Usage: ' + sys.argv[0] + ' csvfile dbfile')
    sys.exit(1)

conn = sqlite3.connect(sys.argv[2])
c = conn.cursor()

def insert(tac, gci, pci, lat, lon, name, conf):
    global c
    record = (
            name,
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

with open(sys.argv[1], 'rt') as csvfile:
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
        for gci_col in ['GCI1', 'GCI2', 'GCI3', 'GCI4']:
            gci = row[cols[gci_col]]
            if gci and '?' not in gci and '999999' not in gci:
                inserted = False
                tac = row[cols['TAC']]
                lat = row[cols['LAT']]
                lon = row[cols['LONG']]
                for pci_col in gci_map:
                    pcis = row[cols[pci_col]]
                    if pcis:
                        for idx, pci in enumerate(pcis.split(',')):
                            pci = pci.strip()
                            if not pci.isdigit():
                                pci = ''
                            if tac == 'x':
                                tac = ''
                            inserted = True
                            insert(tac,
                                    gci[:6] + gci_map[pci_col][idx],
                                    pci,
                                    lat,
                                    lon,
                                    name,
                                    confirmed)
                if not inserted:
                    insert(tac, gci, None, lat, lon, name, confirmed)

conn.commit()
conn.close()
