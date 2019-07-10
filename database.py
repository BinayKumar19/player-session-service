from datetime import datetime

from cassandra.cluster import Cluster
import json
import pandas as pd

class CassandraDB:

    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('sessionkeyspace')
        self.start_events_insert_stmt = self.session.prepare('INSERT INTO start_events(country,player_id,session_id,ts)'
                                                      ' values(?,?,?,?)')
        self.end_events_insert_stmt = self.session.prepare('INSERT INTO end_events(player_id,session_id,ts)'
                                                      ' values(?,?,?)')

        self.events_select_stmt = self.session.prepare('SELECT * FROM end_events WHERE player_id=? LIMIT 20')

    def get_player_events(self, player):
        rows = pd.DataFrame(self.session.execute(self.events_select_stmt, [player]).current_rows)

        return rows.to_json(orient = 'records')

    def save_batch(self, batch):
        print(batch)

        for event in batch:
            print(event['event'])
            time_stamp = datetime.strptime(event['ts'], '%Y-%m-%dT%H:%M:%S')

            if event['event'] == 'start':
                self.session.execute(self.start_events_insert_stmt, (event['country'],event['player_id'],
                                     event['session_id'],time_stamp))
            else:
                self.session.execute(self.end_events_insert_stmt, (event['player_id'], event['session_id'],time_stamp))
        #    self.session.execute(self.event_insert_stmt, [json.dumps(event)])
