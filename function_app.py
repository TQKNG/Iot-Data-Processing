import azure.functions as func
import logging

app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="poc_processing_telemetry",
                               connection="poceventingest_RootManageSharedAccessKey_EVENTHUB") 
def poc_processing_telemetry(azeventhub: func.EventHubEvent):
    logging.info('Python EventHub trigger processed an event: %s',
                azeventhub.get_body().decode('utf-8'))
