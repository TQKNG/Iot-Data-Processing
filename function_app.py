import azure.functions as func
import logging
import json
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

            # Send telemetry to Azure Digital Twin
            adt_client.publish_telemetry(
               digital_twin_id=twin_id,  
               telemetry=json.dumps(telemetry_data)  
            )
            logging.info(f'Telemetry successfully sent to twin {twin_id}')

    except Exception as e:
        logging.error(f"Error processing telemetry: {e}")
   