from flask import Flask, request, jsonify
from database import CassandraDB

app = Flask(__name__)
db = CassandraDB()

@app.route('/fetchsession/<player_id>', methods=['GET'])
def fetch_player_session(player_id):
    #player_id = request.args.get('player_id')
    events = db.get_player_events(player_id)
    print(events)
    return jsonify(events)

@app.route('/batch',  methods=['POST'])
def add_events_batch():
    batch = request.get_json()
    db.save_batch(batch)
 #   print(batch)
    response = None
    return jsonify(response)

if __name__ == '__main__':
    app.run()
