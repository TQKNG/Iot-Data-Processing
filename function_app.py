import azure.functions as func
import logging
import json
from datetime import datetime,timezone, timedelta
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()




@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="poc_processing_telemetry",
                               connection="poceventingest_RootManageSharedAccessKey_EVENTHUB") 
def poc_processing_telemetry(azeventhub: func.EventHubEvent):

    ADT_INSTANCE_URL = 'https://azdtwin.api.eus.digitaltwins.azure.net'
    # Extract then event
    message_body=azeventhub.get_body().decode('utf-8')

    logging.info('Python EventHub trigger processed an event: %s', message_body)

    credential = DefaultAzureCredential()
    adt_client = DigitalTwinsClient(ADT_INSTANCE_URL,credential)

    try:
        # Get twin data
        telemetry = json.loads(message_body)[0]

        telemetry_data = telemetry["data"]["body"]

        # Preset twin_id
        twin_id = 'RobotVirbrix'

        if telemetry_data and twin_id:
            logging.info(f'Telemetry successfully sent to twin {twin_id}')

            # Update telemetry property to Azure Digital Twin
            patch_document =[]
            for key,value in telemetry_data.items():
                if key != 'deviceId' and key !='time':
                    patch_document.append({
                        "op":"replace",
                        "path":f"/{key}",
                        "value":float(value)
                    })
                
                if key =='time':
                    # Current telemetry wrong format ISO8601 2024-12-04T02:22:57.7797990Z
                    # Example: Get the current time in UTC
                    current_time = datetime.now(timezone.utc)

                    # Convert to a specific timezone offset (e.g., -08:00)
                    timezone_offset = timedelta(hours=-8)
                    local_time = current_time.astimezone(timezone(timezone_offset))

                    # Format to RFC 3339 full-time format (time only)
                    rfc3339_time = local_time.strftime("%H:%M:%S%z")
                    rfc3339_time = rfc3339_time[:-2] + ":" + rfc3339_time[-2:]

                    patch_document.append({
                        "op": "replace",
                        "path": "/time",
                        "value": rfc3339_time
                    })

            # patch_document = [
            # {
            #     "op": "replace", 
            #     "path": "/temperature", 
            #     "value": 22.5  # Assuming temperature is a double
            # },
            # {
            #     "op": "replace", 
            #     "path": "/humidity", 
            #     "value": 60  # Assuming humidity is a double
            # }
            # ]

            adt_client.update_digital_twin(twin_id,patch_document)


            logging.info(f'Telemetry successfully sent to twin {twin_id}')

    except Exception as e:
        logging.error(f"Error processing telemetry: {e}")
   