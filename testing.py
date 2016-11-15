from __future__ import print_function

import boto3
import json

print('Loading lambda_handler function')


def lambda_handler(event, context):
    print('Price handler lambda called with: {}'.format(event))

    # Get inputs
    if event.get('body'):
        input_obj = json.loads(event['body'])
    else:
        input_obj = event

    # Assert that we have the input, and build up the Record object
    record_obj = {}
    for required_input in ('vehicles', 'in_operation', 'num_vehicles', 'origin', 'destination', 'miles'):
        assert input_obj.get(required_input), "Input field %s is required" % required_input
        record_obj[required_input] = input_obj.get(required_input)
    print('Record object to send to prediction API: {}'.format(record_obj))

    # Do prediction
    client = boto3.client('machinelearning')
    response = client.predict(
        MLModelId='ml-ZjVE8Q03Qa9',
        Record=record_obj,
        PredictEndpoint="https://realtime.machinelearning.us-east-1.amazonaws.com"
    )
    print('Response from prediction API: {}'.format(response))

    return {'delivery_cost_estimate': response['Prediction']['predictedValue']}
