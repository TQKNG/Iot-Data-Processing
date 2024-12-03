import azure.functions as func
import logging
import json
import jsonpatch
from datetime import datetime
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
            # patch_document =[]
            # for key,value in telemetry_data.items():
            #     if key != 'deviceId' and key !='time':
            #         patch_document.append({
            #             "op":"replace",
            #             "path":f"/{key}",
            #             "value":value
            #         })
                
            #     if key =='time':
            #         time_value = datetime.utcnow().isoformat() + "Z"  # Generate current timestamp in ISO format
            #         patch_document.append({
            #             "op": "replace",
            #             "path": "/time",
            #             "value": time_value
            #         })

       

            # adt_client.update_digital_twin(twin_id,[
            #     {
            #         "op":"replace",
            #         "path":"/temperature",
            #         "value":30
            #     }
            # ])

            patch_document = [
            {
                "op": "replace", 
                "path": "/temperature", 
                "value": 22.5  # Assuming temperature is a double
            },
            {
                "op": "replace", 
                "path": "/humidity", 
                "value": 60  # Assuming humidity is a double
            }
            ]

            adt_client.update_digital_twin(twin_id,patch_document)


            logging.info(f'Telemetry successfully sent to twin {twin_id}')

    except Exception as e:
        logging.error(f"Error processing telemetry: {e}")
   