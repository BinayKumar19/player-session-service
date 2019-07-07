from flask import Flask, request, jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)

@app.route('/fetchsession', methods=['GET'])
def fetch_player_session():
    cluster = Cluster()
    session = cluster.connect('sessionkeyspace')
    rows = session.execute('SELECT event, country, player_id, session_id, ts FROM events')
    print(rows.column_names)
    for user_row in rows:
        print(user_row.event, user_row.country, user_row.player_id, user_row.session_id, user_row.ts)
    return 'Hello World!'

@app.route('/batch',  methods=['POST'])
def add_events_batch():
    batch = request.get_json()
    print(batch)
    response = None
    return jsonify(response)

if __name__ == '__main__':
    app.run()
