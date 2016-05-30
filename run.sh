#!/usr/bin/env bash

[ -n "$1" ] || { echo "Usage: $0 DB"; exit 1; }

set -e

./parse_att.py att_la.csv $1
./parse_br.py att_br.csv $1
./parse_spr.py spr_nola.csv $1
./parse_spr.py spr_la.csv $1
./parse_spr.py spr_etx.csv $1
./parse_tmo.py tmo_la.csv $1
./parse_tmo.py tmo_ms.csv $1
./parse_vzw.py vzw_la.csv $1
