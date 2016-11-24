from __future__ import print_function

import json
import time
import uuid

import boto3

from listings_scraper import get_cleaned_listings_column_groups

S3_BUCKET = "177644182725"


def get_data_schema():
    # Get all fields
    columns_from_raw_to_write, vehicle_types, computed_fields = get_cleaned_listings_column_groups()
    all_fields = columns_from_raw_to_write + vehicle_types + computed_fields

    # Now generate schema based on these lists used for attribute type mapping
    CATEGORICAL_FIELDS = ['listingId', 'formattedCompany', 'delivery.state', 'delivery.city', 'pickup.state',
                          'pickup.city', 'vehicles']
    NUMERIC_FIELDS = ['delivery.latitude', 'delivery.longitude', 'numVehicles', 'pickup.latitude', 'price',
                      'truckMiles', 'rv', 'van', 'travel trailer', 'car', 'atv', 'suv', 'pickup', 'other',
                      'motorcycle', 'heavy equipment', 'boat', 'total_weight', 'pickup.longitude',
                      'price_per_mile']
    BINARY_FIELDS = ['comparable', 'vehicleOperable', 'wideLoad']

    def field_name_to_attribute_type(field_name):
        return 'CATEGORICAL' if field_name in CATEGORICAL_FIELDS else (
            'NUMERIC' if field_name in NUMERIC_FIELDS else (
                'BINARY' if field_name in BINARY_FIELDS else None))

    data_schema_attributes = [
        {'attributeName': field_name, 'attributeType': field_name_to_attribute_type(field_name)} for
        field_name in all_fields
        ]
    if None in [attr_dict['attributeType'] for attr_dict in data_schema_attributes]:
        raise Exception("get_data_schema: No mapping found for a field!")

    return data_schema_attributes


def create_and_evaluate_aml_model(csv_file_name, data_schema_attributes, target_field_name='price',
                                  excluded_variable_names=None):
    """

    data_schema_attributes looks like this: [
                       {"fieldName": "F2", "fieldType": "NUMERIC"},
                       {"fieldName": "F3", "fieldType": "CATEGORICAL"},
                       {"fieldName": "F6", "fieldType": "TEXT"},
                       {"fieldName": "F7", "fieldType": "WEIGHTED_INT_SEQUENCE"},
                       {"fieldName": "F8", "fieldType": "WEIGHTED_STRING_SEQUENCE"}]

    :param str csv_file_name: Filename
    :param List[dict[str, str]] data_schema_attributes: List of dicts with fieldName and fieldType values.
    :param str target_field_name: Target variable
    :param list[str] excluded_variable_names: Columns to exclude
    :return: Response of get_evaluation call, once datasource, model & evaluation are created.
    """

    print("Define data schema")
    # Define datasource name
    uuid4 = str(uuid.uuid4())[0:8]
    ds_name = '{}_{}'.format(uuid4, csv_file_name)
    data_schema = {"version": "1.0",
                   # "recordAnnotationFieldName": 'listingId',
                   # "recordWeightFieldName": None,
                   "rowId": 'listingId',
                   "targetAttributeName": target_field_name,
                   "dataFormat": "CSV",
                   "dataFileContainsHeader": True,
                   "attributes": data_schema_attributes,
                   "excludedVariableNames": excluded_variable_names or []}

    print("Create training datasource in ML")
    ml_client = boto3.client('machinelearning')
    create_training_data_source_response = ml_client.create_data_source_from_s3(
        DataSourceId="training_" + ds_name,
        DataSourceName="training: " + ds_name,
        DataSpec={
            'DataLocationS3': "s3://{}/{}".format(S3_BUCKET, csv_file_name),
            'DataSchema': json.dumps(data_schema),
            'DataRearrangement': json.dumps({"splitting": {"percentBegin": 0, "percentEnd": 80}})
        },
        ComputeStatistics=True)
    print("Create training data source response: ", create_training_data_source_response)

    print("Create prediction datasource in ML")
    create_prediction_data_source_response = ml_client.create_data_source_from_s3(
        DataSourceId="prediction_" + ds_name,
        DataSourceName="prediction: " + ds_name,
        DataSpec={
            'DataLocationS3': "s3://{}/{}".format(S3_BUCKET, csv_file_name),
            'DataSchema': json.dumps(data_schema),
            'DataRearrangement': json.dumps({"splitting": {"percentBegin": 80, "percentEnd": 100}})
        },
        ComputeStatistics=True)
    print("Create prediction data source response: ", create_prediction_data_source_response)

    print("Wait for training and prediction set creations")
    keep_polling(lambda: ml_client.get_data_source(DataSourceId="training_" + ds_name, Verbose=True),
                 lambda first_arg: first_arg['Status'] in ("PENDING", "INPROGRESS"))
    keep_polling(lambda: ml_client.get_data_source(DataSourceId="prediction_" + ds_name, Verbose=True),
                 lambda first_arg: first_arg['Status'] in ("PENDING", "INPROGRESS"))

    print("Create ML model from training set")
    ml_model_id = 'model_{}'.format(ds_name)
    ml_columns = ', '.join([attr['attributeName'] for attr in data_schema_attributes
                            if attr['attributeName'] not in excluded_variable_names])
    ml_model_name = "Columns: {}".format(ml_columns)
    create_ml_model_response = ml_client.create_ml_model(
        MLModelId=ml_model_id,
        MLModelName=ml_model_name,
        MLModelType='REGRESSION',
        Parameters={},
        TrainingDataSourceId="training_" + ds_name)
    print("Create ML model response: ", create_ml_model_response)

    print("Wait until ML model creation is done")
    keep_polling(lambda: ml_client.get_ml_model(MLModelId=ml_model_id, Verbose=True),
                 lambda first_arg: first_arg['Status'] in ('PENDING', 'INPROGRESS'))

    print("Create evaluation from prediction set")
    create_evaluation_response = ml_client.create_evaluation(
        EvaluationId='evaluation_{}'.format(ds_name),
        EvaluationName='evaluation: {}'.format(ds_name),
        MLModelId=ml_model_id,
        EvaluationDataSourceId='prediction_{}'.format(ds_name))
    print("Create evaluation response: ", create_evaluation_response)

    print("Create realtime endpoint for model ", ml_model_id)
    create_realtime_endpoint_response = ml_client.create_realtime_endpoint(MLModelId=ml_model_id)
    print("Create realtime endpoint response: ", create_realtime_endpoint_response)

    print("Wait until evaluation is done")
    keep_polling(lambda: ml_client.get_evaluation(EvaluationId='evaluation_{}'.format(ds_name)),
                 lambda first_arg: first_arg['Status'] in ('PENDING', 'INPROGRESS'))

    get_evaluation_response = ml_client.get_evaluation(EvaluationId='evaluation_{}'.format(ds_name))
    print("RMSE for model {}: {}".format(ml_model_name, get_evaluation_response['PerformanceMetrics']['Properties']['RegressionRMSE']))
    return get_evaluation_response


def keep_polling(first_arg_func, continuation_func_with_first_arg):
    first_arg_func_val = first_arg_func()
    while continuation_func_with_first_arg(first_arg_func_val):
        first_arg_func_val = first_arg_func()
        print("Waiting to get done... ", first_arg_func_val)
        time.sleep(5)


def create_aml_model_with_cleaned_data(included_columns, csv_file_name="csv_files/cleaned_listings.csv", target_field_name='price'):
    """
    Assumes cleaned_listings.csv is already uploaded to S3, and has all columns. Sets target field name to 'price'.
    Only includes columns in data_schema_attributes
    :return:
    """

    # Given included columns, get excluded columns to pass to AML
    data_schema_attributes = get_data_schema()
    all_columns = [attr_dict['attributeName'] for attr_dict in data_schema_attributes]
    excluded_columns = list(set(all_columns) - set(included_columns))

    create_and_evaluate_aml_model(csv_file_name,
                                  data_schema_attributes=data_schema_attributes,
                                  target_field_name=target_field_name,
                                  excluded_variable_names=excluded_columns)
