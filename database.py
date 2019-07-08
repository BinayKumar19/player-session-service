from cassandra.cluster import Cluster
import json
import pandas as pd

class CassandraDB:

    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('sessionkeyspace')
        self.event_insert_stmt = self.session.prepare('INSERT INTO events JSON ?')
        self.event_select_stmt = self.session.prepare('SELECT * FROM events WHERE player_id=?')

    def get_player_events(self, player):
        rows = pd.DataFrame(self.session.execute(self.event_select_stmt, [player]).current_rows)
        rows_group = rows.groupby(['session_id']).size() == 2
        rows_group2 = rows.groupby('session_id').size() == 2

     #   rows_group3 = rows.groupby('session_id').filter(lambda x: (x.count() != 2))
        rows_group3 = rows.groupby('session_id').size().filter(lambda x: print('x:',x))

        print(rows_group3)
    #    rows_group4 = rows.groupby('session_id').size()[1]
    #    print(rows_group4)

        print('----------------------')
        print(rows_group)
        print(rows_group2)
        return rows

    def save_batch(self, batch):
        print(batch)
        for event in batch:
            self.session.execute(self.event_insert_stmt, [json.dumps(event)])
