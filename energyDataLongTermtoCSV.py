import websocket
import json
import pandas as pd
from datetime import datetime, timedelta
import os

access_token = "your long-lived access token here
home_assistant_url = "ws://your.ha.instance:8123/api/websocket"
statistic_ids = [
    "sensor.grid_incoming", "sensor.grid_incoming_cost",
    "sensor.grid_outgoing", "sensor.batt_out", "sensor.batt_in",
    "sensor.solar_dc1", "sensor.solar_dc2"
]

# Initialize date range for data requests
start_date = datetime.now()
end_date = start_date - timedelta(days=730)  # Adjust as needed
message_id = 1

def on_message(ws, message):
    global start_date, message_id  # Use global variables to update them
    
    try:
        data = json.loads(message)
        print("Received message: ", data)
        message_type = data.get('type')

        # Handle different types of messages
        if message_type == 'auth_required':
            # Server requests authentication, send the token
            auth_data = {
                "type": "auth",
                "access_token": access_token
            }
            ws.send(json.dumps(auth_data))
        elif message_type == 'auth_ok':
            # Authentication successful, now send your data request
            send_data_request(ws, start_date, message_id)
        elif message_type == 'result':
            if data.get('success', False):
                if 'result' in data:
                    # Now it's safe to process the result
                    print("Processing result...")
                    process_data(data.get('result'))
                    message_id += 1
                    # Adjust for the next request if needed
                    start_date -= timedelta(days=1)
                    if start_date >= end_date:  # Modify based on your logic
                        send_data_request(ws, start_date, message_id)
                    else:
                        ws.close()
                else:
                    # Log when 'result' is expected but missing
                    print(f"Either unsuccessful or 'result' key missing. Data: {data}")
            else:
                # Handle errors in result
                error_info = data.get('error', {})
                print(f"Error processing message: {error_info.get('code')}, {error_info.get('message')}")
        else:
            print(f"Unhandled message type: {message_type}")
    except KeyError as e:
        print(f"KeyError accessing message data: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def send_data_request(ws, date, msg_id):
    # Format start and end times in ISO 8601
    start_iso = date.strftime('%Y-%m-%dT00:00:00.000Z')
    end_iso = date.strftime('%Y-%m-%dT23:59:59.999Z')

    data_request = {
        "id": msg_id,
        "type": "recorder/statistics_during_period",
        "start_time": start_iso,
        "end_time": end_iso,
        "statistic_ids": statistic_ids,
        "period": "hour",
        "units": {"energy": "kWh", "volume": "m³"},
        "types": ["change"]
    }
    print(json.dumps(data_request))
    ws.send(json.dumps(data_request))

def process_data(data):
    # Initialize a list to store processed records
    # Check if the CSV file already exists to decide on writing headers later
    file_exists = os.path.isfile('solar_data.csv')
    try:
        # This list will hold all rows of our dataframe
        processed_data = []
        
        # Iterate through each sensor's data
        for sensor_id, readings in data.items():
            for reading in readings:
                # Convert start and end timestamps from Unix milliseconds to readable format
                start = datetime.utcfromtimestamp(reading['start'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                end = datetime.utcfromtimestamp(reading['end'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                change = reading['change']
                
                # Append the data for this reading as a new row in our list
                processed_data.append({
                    'sensor_id': sensor_id,
                    'start': start,
                    'end': end,
                    'change': change
                })
        
        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(processed_data)
        
        # Pivot table to have sensor IDs as columns, and their changes as values
        df_pivot = df.pivot_table(index=['start', 'end'], columns='sensor_id', values='change', aggfunc='first').reset_index()
        
        # Save to CSV
        df_pivot.to_csv('solar_data.csv', mode='a', header=not file_exists, index=False)
    except Exception as e:
        print(f"process_data error: {e}")

def on_error(ws, error):
    print("Error: ", error)

def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")

def on_open(ws):
    auth_data = {
        "type": "auth",
        "access_token": access_token
    }
    #ws.send(json.dumps(auth_data))

if __name__ == "__main__":
    ws = websocket.WebSocketApp(home_assistant_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()  # Note: Be cautious with ssl.CERT_NONE in production environments
