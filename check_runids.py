#!/usr/bin/env python3
import sqlite3
conn = sqlite3.connect('data/runtime/ledger.db')
c = conn.cursor()
c.execute('SELECT run_id, COUNT(*) as cnt, MIN(event_ts), MAX(event_ts) FROM ledger_events GROUP BY run_id ORDER BY MAX(event_ts) DESC LIMIT 5')
for row in c.fetchall():
    print(f'run_id={row[0]}, count={row[1]}, start={row[2]}, end={row[3]}')
conn.close()