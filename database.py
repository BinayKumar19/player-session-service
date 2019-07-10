from datetime import datetime,timedelta
from cassandra.cluster import Cluster
import pandas as pd


class CassandraDB:

    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('sessionkeyspace')
        self.start_events_insert_stmt = self.session.prepare('INSERT INTO start_events(country,player_id,session_id,ts)'
                                                             ' values(?,?,?,?)')
        self.end_events_insert_stmt = self.session.prepare('INSERT INTO end_events(player_id,session_id,ts)'
                                                           ' values(?,?,?)')

        self.events_select_stmt = self.session.prepare('SELECT JSON * FROM end_events WHERE player_id=? and ts > ? LIMIT 20')

    def get_player_events(self, player):
        current_time = datetime.now()- timedelta(days=365)
        # current_time = datetime.strptime(str(now), '%Y-%m-%dT%H:%M:%S')

        rows = pd.DataFrame(self.session.execute(self.events_select_stmt, (player,current_time)).current_rows)

#        return rows.to_json(orient='records')
        #        {'split','records','index','columns','values','table'}
        return rows.to_json(orient='values')

    def save_batch(self, batch):

        for event in batch:
            print(event['event'])
            time_stamp = datetime.strptime(event['ts'], '%Y-%m-%dT%H:%M:%S')

            if event['event'] == 'start':
                self.session.execute(self.start_events_insert_stmt, (event['country'], event['player_id'],
                                                                     event['session_id'], time_stamp))
            else:
                self.session.execute(self.end_events_insert_stmt, (event['player_id'], event['session_id'], time_stamp))
