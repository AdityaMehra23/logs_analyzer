from flask import Flask, jsonify, make_response
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

app = Flask(__name__)

# Cassandra connection and session with Cluster ip
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('sparkanalysis')
session.row_factory = dict_factory  # Use dictionary-based rows for easier JSON conversion

@app.route('/response_codes_today', methods=['GET'])
def get_response_codes_today():
    try:
        # Execute the Cassandra query to fetch response codes for today
        query = """
            SELECT process_timestamp, response_code, count(*) as total_count
            FROM web_access_time
            WHERE process_date = toDate(now())
            GROUP BY process_timestamp, response_code
            ORDER BY process_timestamp DESC
            LIMIT 10
        """
        query2 = """
            SELECT process_timestamp, response_code, count(*) as total_count
            FROM web_access_time
            WHERE process_date = '2023-09-10'
            GROUP BY process_timestamp, response_code
            ORDER BY process_timestamp DESC
            LIMIT 10
        """
        result = session.execute(query2)
        rows = result.all()
        if len(rows)==0:
            # Return No data available      
            return make_response(jsonify(message= 'No data available for the day'), 400)
    
        # Return the data as JSON
        return jsonify(rows)
    except Exception as e:
        return make_response(jsonify(message= str(e)), 500)

@app.route('/output_bytes_today', methods=['GET'])
def get_output_bytes_today():
    try:
        # Execute the Cassandra query to fetch total output bytes for today
        query = """
            SELECT process_timestamp, sum(response_bytes) as output_bytes
            FROM web_access_time
            WHERE process_date = toDate(now())
            GROUP BY process_timestamp
            ORDER BY process_timestamp DESC
            LIMIT 10
        """
        query2 = """
            SELECT process_timestamp, sum(response_bytes) as output_bytes
            FROM web_access_time
            WHERE process_date = '2023-09-10'
            GROUP BY process_timestamp
            ORDER BY process_timestamp DESC
            LIMIT 10
        """
        result = session.execute(query2)
        rows = result.all()
        if len(rows)==0:
            # Return No data available      
            return make_response(jsonify(message= 'No data available for the day'), 400)
    
        # Return the data as JSON
        return jsonify(rows)
    except Exception as e:
        return make_response(jsonify(message= str(e)), 500)

@app.route('/total_requests_today', methods=['GET'])
def get_total_requests_today():
    try:
        # Execute the Cassandra query to fetch total requests for today
        query = """
            SELECT process_timestamp, count(*) as total_requests
            FROM web_access_time
            WHERE process_date = toDate(now())
            GROUP BY process_timestamp
            ORDER BY process_timestamp DESC
            LIMIT 10
        """
        query2 = """
            SELECT process_timestamp, count(*) as total_requests
            FROM web_access_time
            WHERE process_date = '2023-09-10'
            GROUP BY process_timestamp
            ORDER BY process_timestamp DESC
            LIMIT 10
        """
        result = session.execute(query)
        rows = result.all()
        
        if len(rows)==0:
            # Return No data available       
            return make_response(jsonify(message= 'No data available for the day'), 400)
    
        # Return the data as JSON
        return jsonify(rows)
    except Exception as e:
        return make_response(jsonify(message= str(e)), 500)

if __name__ == '__main__':
    app.run(debug=True)
