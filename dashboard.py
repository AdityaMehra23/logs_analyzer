from dash import Dash, dcc, html, Input, Output
import requests
import plotly.express as px
import pandas as pd


class CustomException(Exception):
    # we create a CustomException for some cases
    def __init__(self, message):
        super().__init__(message)

app = Dash(__name__)

app.layout = html.Div([
    dcc.Interval(id='interval-component', interval=10*1000, n_intervals=0),  # Refresh every 10 seconds
    dcc.Graph(id='total-requests'),
    dcc.Graph(id='response-codes'),
    dcc.Graph(id='bytes-transferred')
])

@app.callback(
    [Output('total-requests', 'figure'),
     Output('response-codes', 'figure'),
     Output('bytes-transferred', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_graphs(n_intervals):
    try:
        # GET requests to Flask REST APIs to fetch the data
        
        total_requests_url = 'http://localhost:5000/total_requests_today'
        response_codes_url = 'http://localhost:5000/response_codes_today'
        bytes_transferred_url = 'http://localhost:5000/output_bytes_today'
        
        total_requests_response = requests.get(total_requests_url)
        response_codes_response = requests.get(response_codes_url)
        bytes_transferred_response = requests.get(bytes_transferred_url)
        

        
        try:
            # Bar chart for total requests per timestamp
            if total_requests_response.status_code!=200:
                raise CustomException("No data available")
            total_requests_data = total_requests_response.json()
            df_requests = pd.DataFrame(total_requests_data)
            total_requests_chart = px.bar(df_requests, x='process_timestamp', y='total_requests', title='Total Requests')
        except Exception as e:
            total_requests_chart = create_error_chart(str(e))
        
        try:
            # Bar chart for response codes' count per timestamp
            if response_codes_response.status_code!=200:
                raise CustomException("No data available")
            response_codes_data = response_codes_response.json()
        
            df_responses= pd.DataFrame(response_codes_data)
            response_codes_chart = px.bar(df_responses, x='response_code', y='total_count', color='response_code',
                 labels={'response_code': 'Response Code', 'total_count': 'Total Count'},
                 title='Response Code Distribution by Timestamp')
        except Exception as e:
            response_codes_chart = create_error_chart(str(e))
        
        try:
            # Bar chart for total bytes per timestamp
            if bytes_transferred_response.status_code!=200:
                raise CustomException("No data available")
            bytes_transferred_data = bytes_transferred_response.json()
            df_bytes = pd.DataFrame(bytes_transferred_data)
            bytes_transferred_chart = px.bar(df_bytes , x='process_timestamp', y='output_bytes', title='Bytes Transferred')
        except Exception as e:
            bytes_transferred_chart = create_error_chart(str(e))
        
        return total_requests_chart, response_codes_chart, bytes_transferred_chart
    
    
    except Exception as e:
        return create_error_chart(str(e)),create_error_chart(str(e)),create_error_chart(str(e))

def create_error_chart(error_message):
    
    return {'data': [{'x': [], 'y': [], 'type': 'bar', 'text': error_message}],
            'layout': {'title': error_message, 'xaxis': {'title': '', 'showticklabels': False}}}

if __name__ == '__main__':
    app.run_server(debug=True)
