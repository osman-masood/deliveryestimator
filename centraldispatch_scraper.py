from __future__ import print_function

import argparse
import csv
import json
import re
import time
import uuid
from pprint import pprint

import boto3
import requests

from estimator import convert_vehicles_str_to_weight, raw_vehicles_str_to_vehicle_type_to_count

S3_BUCKET = "177644182725"

CSRF_TOKEN = "c12a9b0cd347de85e49450d0c9f4e156c7234e8dd8102f6f5446b1302dbf00b7"

PAGE_URL = "https://www.centraldispatch.com/protected/listing-search/result?routeBased=0&corridorWidth=&routePickupCity=&routePickupState=&routePickupZip=&route_origination_valid=&routeDeliveryCity=&routeDeliveryState=&routeDeliveryZip=&route_destination_valid=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypoint_valid=1&pickupCity=&pickupRadius=25&pickupState=&pickupZip=&pickupAreas%5B%5D=state_USA_CA&origination_valid=1&deliveryCity=&deliveryRadius=25&deliveryState=&deliveryZip=&deliveryAreas%5B%5D=state_USA_CA&destination_valid=1&FatAllowCanada=1&vehicleTypeIds%5B%5D=&trailerType=&vehiclesRun=&minVehicles=1&maxVehicles=&shipWithin=60&paymentType=&minPayPrice=&minPayPerMile=&highlightOnTop=0&postedBy=&highlightPeriod=0&listingsPerPage=500&primarySort=1&secondarySort=4&filterBlocked=0&highlightPreferred=0&CSRFToken={}".format(
    CSRF_TOKEN)
AJAX_URL = "https://www.centraldispatch.com/protected/listing-search/get-results-ajax?routeBased=0&corridorWidth=&routePickupCity=&routePickupState=&routePickupZip=&route_origination_valid=&routeDeliveryCity=&routeDeliveryState=&routeDeliveryZip=&route_destination_valid=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypoint_valid=1&pickupCity=&pickupRadius=25&pickupState=&pickupZip=&pickupAreas%5B%5D=All&origination_valid=1&deliveryCity=&deliveryRadius=25&deliveryState=&deliveryZip=&deliveryAreas%5B%5D=All&destination_valid=1&FatAllowCanada=1&vehicleTypeIds%5B%5D=&trailerType=&vehiclesRun=&minVehicles=1&maxVehicles=&shipWithin=60&paymentType=&minPayPrice=&minPayPerMile=&highlightOnTop=0&postedBy=&highlightPeriod=0&listingsPerPage=500&primarySort=1&secondarySort=4&filterBlocked=0&highlightPreferred=0&CSRFToken={}&pageStart={}&pageSize=100&template=1"

COLUMNS_TO_SCRAPE = ['listingId', 'formattedCompany', 'comparable',
                     'delivery.state', 'delivery.latitude', 'delivery.longitude', 'delivery.city',
                     'numVehicles', 'vehiclesCount', 'vehicleOperable', 'vehicles', 'vehicle_make', 'vehicle_types',
                     'wideLoad',
                     'pickup.state', 'pickup.latitude', 'pickup.longitude', 'pickup.city',
                     'preferred_shipper', 'price', 'truckMiles',
                     'rating_count', 'rating_license', 'rating_score']


def listing_value_from_column_name(listing, column_name):
    column_value = raw_listing_value_from_column_name(listing, column_name)

    # Perform manual type conversions
    if column_name == 'rating_license':
        column_value = 1 if column_value == 'valid' else 0
    elif column_name in ('preferred_shipper', 'wideLoad', 'vehicleOperable', 'comparable'):
        # Convert binary columns
        column_value = 1 if column_value else 0
    elif column_name in ('rating_score', 'truckMiles', 'rating_count'):
        # Convert numerical columns
        try:
            column_value = float(column_value.replace('%', ''))
        except ValueError:
            column_value = None
    elif column_name == 'vehicles':  # TODO. Keep vehicles. Should not be replaced but rather be a second column
        # Vehicles is actually completely converted to total_weight
        column_value = column_value.replace('(', '').replace(')', '')  # Remove surrounding ()
        column_value = convert_vehicles_str_to_weight(column_value)

    return column_value


def raw_listing_value_from_column_name(listing, column_name):
    if '.' in column_name:
        first_col, second_col = column_name.split('.')
        column_value = listing[first_col].get(second_col, None)
    else:
        column_value = listing.get(column_name, None)

    return column_value


def scrape_central_dispatch():
    has_more_listings = True
    page_start = 0
    with open('raw_data.csv', 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(COLUMNS_TO_SCRAPE)
        while has_more_listings:
            print("AJAX call with page_start=", page_start)
            response = requests.get(AJAX_URL.format(CSRF_TOKEN, page_start),
                                    headers={
                                        "Accept": "application/json, text/javascript, */*; q=0.01",
                                        "Accept-Encoding": "gzip, deflate, sdch, br",
                                        "Accept-Language": "en-US,en;q=0.8,sv;q=0.6",
                                        "Cache-Control": "no-cache",
                                        "Connection": "keep-alive",
                                        "Cookie": "CSRF_TOKEN=c12a9b0cd347de85e49450d0c9f4e156c7234e8dd8102f6f5446b1302dbf00b7; visitedDashboard=1; defaultView=list; test-persistent=1; test-session=1; PHPSESSID=9506610c41374e42cdc5b0c97ce96f55",
                                        "Host": "www.centraldispatch.com",
                                        "Pragma": "no-cache",
                                        "Upgrade-Insecure-Requests": "1",
                                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36",
                                        "Referer": PAGE_URL,
                                        "X-NewRelic-ID": "VQcFUFBUCxAJUFFSAQQO",
                                        "X-Requested-With": "XMLHttpRequest"
                                    }).json()

            print("Retrieved {} listings, and writing them to raw_data.csv".format(len(response['listings'])))
            for i, listing in enumerate(response['listings']):
                row_values = map(lambda column_name: raw_listing_value_from_column_name(listing, column_name),
                                 COLUMNS_TO_SCRAPE)
                # row_values = [listing[column_name] for column_name in COLUMNS_TO_SCRAPE]
                try:
                    csv_writer.writerow(row_values)
                except UnicodeEncodeError as e:
                    print("Error writing row values {} of line {}: {}".format(row_values, page_start + i, e))

            # Increment state vars
            page_start += 500
            has_more_listings = len(response['listings']) > 0
            time.sleep(1)

    print("Done!")


def get_num_vehicles_from_vehicle_make(vehicle_make):
    """

    :param vehicle_make:  As described in analyze_raw_data_csv().
    :return:
    """
    makes = filter(None, map(str.strip, vehicle_make.split('\n')))
    vehicle_count = 0
    for make in makes:
        # (9) mixed units
        match_groups = re.search(r'\((\d+)\)', make)
        # 2016 10 units f-150 crew cabs 145''w/b will split 5 an 5
        x_units_match = re.search(r'(\d+)\s+units', make)

        if match_groups and len(match_groups.groups()) > 0:
            vehicle_count += int(match_groups.groups()[0])
        elif x_units_match and len(x_units_match.groups()) > 0:
            vehicle_count += int(x_units_match.groups()[0])
        else:
            # 2011 nissan Sentra
            vehicle_count += 1
    # print("get_num_vehicles_from_vehicle_make({}) => {}".format(vehicle_make, vehicle_count))
    return vehicle_count


def analyze_raw_data_csv():
    """
    Analyzes rawdata.csv (output of scraping CentralDispatch) and prints results.

    Analysis results:
    -Use numVehicles instead of vehiclesCount
    -preferred_shipper is useless (all False)
    -Don't include 'comparable' as that shouldn't affect pricing
    -wideLoad is all '0'
    -vehicle_make is newline-separated strings like:
        2011 nissan sentra
        2011 ram 1500- inop no keys
        2014 mitsubishi outlander sport
        2016 ram 1500
        2016 10 units f-150 crew cabs 145''w/b will split 5 an 5
        (9) mixed units
        2000 terex tr100 3 units total
        (3) 2016 mixed units
        Note: Sometimes the number of "units" in vehicle_make is not reflected in numVehicles.
    -truckMiles can sometimes be -1: Should use google maps API to get miles?

    """
    col_name_to_value_to_count = {
        'comparable': {},
        'numVehicles': {},
        'preferred_shipper': {},
        'vehicleOperable': {},
        'vehicles': {},
        'vehicle_types': {},
        'vehicle_make': {},
        'truckMiles': {},
        'price': {}
    }  # Maps field name to value to # of records w/ that value

    with open('raw_data.csv', 'rb') as csvfile:
        csv_reader = csv.DictReader(csvfile, fieldnames=COLUMNS_TO_SCRAPE)
        for row in csv_reader:
            for col_name in col_name_to_value_to_count.keys():
                col_name_to_value_to_count[col_name].setdefault(row[col_name], 0)
                col_name_to_value_to_count[col_name][row[col_name]] += 1

    print("col_name_to_value_to_count: \n")
    pprint(col_name_to_value_to_count)


def get_cleaned_column_groups():
    columns_from_raw_to_write = [
        'listingId', 'formattedCompany', 'comparable', 'delivery.state', 'delivery.latitude',
        'delivery.longitude', 'delivery.city', 'numVehicles', 'vehicleOperable', 'wideLoad',
        'pickup.state', 'pickup.latitude', 'pickup.longitude', 'pickup.city', 'price', 'truckMiles',
    ]

    # Also, one column per type of vehicle (pickup, van, etc) containing # of that type of vehicle
    vehicle_types = ['rv', 'van', 'travel trailer', 'car', 'vehicles', 'atv', 'suv', 'pickup', 'other', 'motorcycle',
                     'heavy equipment', 'boat']

    # And, one column for total_weight
    total_weight = ['total_weight']

    return columns_from_raw_to_write, vehicle_types, total_weight


def get_data_schema():
    # Get all fields
    columns_from_raw_to_write, vehicle_types, total_weight = get_cleaned_column_groups()
    all_fields = columns_from_raw_to_write + vehicle_types + total_weight

    # Now generate schema based on these lists used for attribute type mapping
    CATEGORICAL_FIELDS = ['listingId', 'formattedCompany', 'delivery.state', 'delivery.city', 'pickup.state',
                          'pickup.city']
    NUMERIC_FIELDS = ['delivery.latitude', 'delivery.longitude', 'numVehicles', 'pickup.latitude', 'price',
                      'truckMiles', 'rv', 'van', 'travel trailer', 'car', 'vehicles', 'atv', 'suv', 'pickup', 'other',
                      'motorcycle', 'heavy equipment', 'boat', 'total_weight', 'pickup.longitude']
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


def clean_raw_data():
    """
    Writes into cleaned_data.csv which will be datasource of ML model.
    """
    columns_from_raw_to_write, vehicle_types, total_weight = get_cleaned_column_groups()
    columns_to_write = columns_from_raw_to_write + vehicle_types + total_weight
    output_file = "cleaned_data.csv"

    with open('raw_data.csv', 'rb') as raw_csv_file:
        raw_csv_reader = csv.DictReader(raw_csv_file, fieldnames=COLUMNS_TO_SCRAPE)
        with open(output_file, 'wb') as cleaned_csv_file:
            cleaned_csv_writer = csv.DictWriter(cleaned_csv_file, fieldnames=columns_to_write)
            cleaned_csv_writer.writeheader()

            for raw_data_dict in raw_csv_reader:
                # If the truckMiles <= 0 or price is <= 10, it's completely invalid, so skip it.
                try:
                    if int(raw_data_dict.get('truckMiles')) <= 0 or int(raw_data_dict.get('price')) <= 10:
                        print("Skipped invalid truckMiles/price for listing ", raw_data_dict)
                        continue
                except:
                    print("Skipped invalid truckMiles/price for listing ", raw_data_dict)
                    continue

                # Copy over the certain columns from raw
                cleaned_dict = {column_name: raw_data_dict.get(column_name) for column_name in
                                columns_from_raw_to_write}

                # For each possible vehicle type, add its count as a column
                vehicle_type_to_count = raw_vehicles_str_to_vehicle_type_to_count(raw_data_dict['vehicles'])
                for vehicle_type in vehicle_types:
                    cleaned_dict[vehicle_type] = vehicle_type_to_count.get(vehicle_type, 0)

                # Add the total weight based on 'vehicles' as well
                cleaned_dict['total_weight'] = convert_vehicles_str_to_weight(raw_data_dict['vehicles'])

                # If the vehicles count from vehicle_make is greater than numVehicles, use that instead
                num_vehicles_from_make = get_num_vehicles_from_vehicle_make(raw_data_dict['vehicle_make'])
                if num_vehicles_from_make > int(cleaned_dict['numVehicles']):
                    print("Vehicles from make is greater than numVehicles ({}): {} > {}".format(
                        raw_data_dict['vehicle_make'], num_vehicles_from_make, cleaned_dict['numVehicles']
                    ))
                    cleaned_dict['numVehicles'] = num_vehicles_from_make

                # Write the row
                cleaned_csv_writer.writerow(cleaned_dict)


def get_vehicle_types_from_raw_data():
    """
    Output of this is currently:
    ['rv', 'van', 'travel trailer', 'car', 'vehicles', 'atv', 'suv', 'pickup', 'other', 'motorcycle',
    'heavy equipment', 'boat']
    :return:
    """
    vehicle_types = set()
    with open('raw_data.csv', 'rb') as raw_csv_file:
        raw_csv_reader = csv.DictReader(raw_csv_file, fieldnames=COLUMNS_TO_SCRAPE)
        for row_dict in raw_csv_reader:
            print("Vehicle types: ", row_dict['vehicles'])
            vehicle_types |= set(raw_vehicles_str_to_vehicle_type_to_count(row_dict['vehicles']).keys())
    print("Vehicle types: ", vehicle_types)


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
    :return:
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
            'DataRearrangement': json.dumps({"splitting": {"percentBegin": 20, "percentEnd": 100}})
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
    create_ml_model_response = ml_client.create_ml_model(
        MLModelId=ml_model_id,
        MLModelName="Columns: {}".format(ml_columns),
        MLModelType='REGRESSION',
        Parameters={},
        TrainingDataSourceId="training_" + ds_name)
    print("Create ML model response: ", create_ml_model_response)

    print("Wait until ML model creation is done")
    keep_polling(lambda: ml_client.get_ml_model(
        MLModelId=ml_model_id,
        Verbose=True), lambda first_arg: first_arg['Status'] in ('PENDING', 'INPROGRESS'))

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


def keep_polling(first_arg_func, continuation_func_with_first_arg):
    first_arg_func_val = first_arg_func()
    while continuation_func_with_first_arg(first_arg_func_val):
        print("Waiting to get done... ", first_arg_func_val)
        first_arg_func_val = first_arg_func()
        time.sleep(5)


def create_aml_model_with_cleaned_data(included_columns, csv_file_name="cleaned_data.csv"):
    """
    Assumes cleaned_data.csv is already uploaded to S3, and has all columns. Sets target field name to 'price'.
    Only includes columns in data_schema_attributes
    :return:
    """

    # Given included columns, get excluded columns to pass to AML
    data_schema_attributes = get_data_schema()
    all_columns = [attr_dict['attributeName'] for attr_dict in data_schema_attributes]
    excluded_columns = list(set(all_columns) - set(included_columns))

    create_and_evaluate_aml_model(csv_file_name,
                                  data_schema_attributes=data_schema_attributes,
                                  target_field_name='price',
                                  excluded_variable_names=excluded_columns)


parser = argparse.ArgumentParser(description='Create an AWS Lambda Deployment')
parser.add_argument('--clean_raw_data', action='store_true')
parser.add_argument('--upload_cleaned_data', action='store_true')
parser.add_argument('--create_aml_model', action='store_true')
args = parser.parse_args()

if args.clean_raw_data:
    clean_raw_data()

if args.upload_cleaned_data:
    print("Uploading file cleaned_data.csv to S3 into bucket ", S3_BUCKET)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)
    bucket.upload_file("cleaned_data.csv", "cleaned_data.csv")

if args.create_aml_model:
    create_aml_model_with_cleaned_data(
        included_columns=['truckMiles', 'listingId', 'price', 'numVehicles', 'total_weight'])


# if __name__ == "__main__":
# scrape_central_dispatch()
# analyze_raw_data_csv()
# clean_raw_data()
# get_vehicle_types_from_raw_data()

# data_schema_attributes = [
#     {"fieldName": "truckMiles", "fieldType": "NUMERIC"},
#     {"fieldName": "price", "fieldType": "NUMERIC"},
#     {"fieldName": "total_weight", "fieldType": "NUMERIC"},
# ]
# create_aml_model_with_cleaned_data(data_schema_attributes)


# assert convert_vehicles_str_to_weight("(Car)") == 3497
# assert convert_vehicles_str_to_weight("(4 Car, Pickup)") > 5000
