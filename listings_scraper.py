import csv
import re
import time
from random import randint

import requests

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


def scrape_central_dispatch_listings():
    has_more_listings = True
    page_start = 0
    with open('csv_files/raw_listings.csv', 'wb') as csvfile:
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
                                        "Cookie": "CSRF_TOKEN={}; visitedDashboard=1; defaultView=list; test-persistent=1; test-session=1; PHPSESSID=d9d8aff688df964876b9bf61b4d51c5a".format(CSRF_TOKEN),
                                        "Host": "www.centraldispatch.com",
                                        "Pragma": "no-cache",
                                        "Upgrade-Insecure-Requests": "1",
                                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36",
                                        "Referer": PAGE_URL,
                                        "X-NewRelic-ID": "VQcFUFBUCxAJUFFSAQQO",
                                        "X-Requested-With": "XMLHttpRequest"
                                    }).json()

            print("Retrieved {} listings, and writing them to raw_listings.csv".format(len(response['listings'])))
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
            time.sleep(10 + randint(5, 10))

    print("Done!")


def get_cleaned_listings_column_groups():
    columns_from_raw_to_write = [
        'listingId', 'formattedCompany', 'comparable', 'delivery.state', 'delivery.latitude',
        'delivery.longitude', 'delivery.city', 'numVehicles', 'vehicleOperable', 'wideLoad', 'vehicles',
        'pickup.state', 'pickup.latitude', 'pickup.longitude', 'pickup.city', 'price', 'truckMiles',
    ]

    # Also, one column per type of vehicle (pickup, van, etc) containing # of that type of vehicle
    vehicle_types = ['rv', 'van', 'travel trailer', 'car', 'atv', 'suv', 'pickup', 'other', 'motorcycle',
                     'heavy equipment', 'boat']

    # And, the computed fields
    computed_fields = ['total_weight', 'price_per_mile']

    return columns_from_raw_to_write, vehicle_types, computed_fields


def clean_raw_data():
    """
    Writes into cleaned_listings.csv which will be datasource of ML model.
    """
    columns_from_raw_to_write, vehicle_types, computed_fields = get_cleaned_listings_column_groups()
    columns_to_write = columns_from_raw_to_write + vehicle_types + computed_fields
    output_file = "csv_files/cleaned_listings.csv"

    with open('csv_files/raw_listings.csv', 'rb') as raw_csv_file:
        raw_csv_reader = csv.DictReader(raw_csv_file, fieldnames=COLUMNS_TO_SCRAPE)
        with open(output_file, 'wb') as cleaned_csv_file:
            cleaned_csv_writer = csv.DictWriter(cleaned_csv_file, fieldnames=columns_to_write)
            cleaned_csv_writer.writeheader()

            for raw_data_dict in raw_csv_reader:
                # If the truckMiles <= 0 or price is <= 10, it's completely invalid, so skip it.
                try:
                    if float(raw_data_dict.get('truckMiles')) <= 0.0 or float(raw_data_dict.get('price')) <= 10.0:
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

                # If there is an 'other' type of vehicle, skip this row. We won't process 'others' for now
                if cleaned_dict.get('other', 0) > 0:
                    print("Skipping row that has 'other' vehicle type: ", raw_data_dict['vehicles'])
                    continue

                # Add the total weight based on 'vehicles' as well
                cleaned_dict['total_weight'] = convert_vehicles_str_to_weight(raw_data_dict['vehicles'])

                # Add price per mile
                cleaned_dict['price_per_mile'] = float(raw_data_dict.get('price')) / float(
                    raw_data_dict.get('truckMiles'))

                # If the vehicles count from vehicle_make is greater than numVehicles, use that instead
                num_vehicles_from_make = get_num_vehicles_from_vehicle_make(raw_data_dict['vehicle_make'])
                if num_vehicles_from_make > int(cleaned_dict['numVehicles']):
                    print("Vehicles from make is greater than numVehicles ({}): {} > {}".format(
                        raw_data_dict['vehicle_make'], num_vehicles_from_make, cleaned_dict['numVehicles']
                    ))
                    cleaned_dict['numVehicles'] = num_vehicles_from_make

                # Write the row
                cleaned_csv_writer.writerow(cleaned_dict)


def raw_vehicles_str_to_vehicle_type_to_count(vehicles_str):
    """
    "(Car, Pickup, 3 SUV)" -> {'car': 1, 'pickup': 1, 'suv': 3}

    :param vehicles_str:
    :return:
    """
    if not vehicles_str:
        return {}

    # Remove ( and ) chars
    vehicles_str = vehicles_str.replace('(', '').replace(')', '').lower()

    # Split on comma, remove empties, and strip each one
    vehicles_in_str = map(str.strip, filter(None, vehicles_str.split(',')))

    vehicle_type_to_count = {}
    for vehicle_str in vehicles_in_str:
        multiplier = 1

        # Handle cases like "2 Car": Figure out multiplier
        match = re.search('(\d+)\s+(.*)', vehicle_str)
        if match and len(match.groups()) == 2:
            # print("Match of %s is broken into %s and %s" % (vehicle_str, match.group(1), match.group(2)))
            multiplier = int(match.group(1))
            vehicle_str = match.group(2)

        vehicle_type_to_count.setdefault(vehicle_str, 0)
        vehicle_type_to_count[vehicle_str] += multiplier
    return vehicle_type_to_count


def convert_vehicles_str_to_weight(vehicles_str):
    if not vehicles_str:
        return None

    # Remove ( and ) chars
    vehicles_str = vehicles_str.replace('(', '').replace(')', '').lower()

    total_weight = 0
    vehicles_in_str = filter(None, vehicles_str.split(','))
    for vehicle_str in vehicles_in_str:
        vehicle_str = vehicle_str.strip()
        multiplier = 1

        # Handle cases like "2 Car": Figure out multiplier
        match = re.search('(\d+)\s+(.*)', vehicle_str)
        if match and len(match.groups()) == 2:
            # print("Match of %s is broken into %s and %s" % (vehicle_str, match.group(1), match.group(2)))
            multiplier = int(match.group(1))
            vehicle_str = match.group(2)

        # Now calculate the weight of the vehicle using multiplier & weight mapping
        weight_of_vehicle = VEHICLE_TO_WEIGHT.get(vehicle_str.lower())
        if weight_of_vehicle:
            # print("Weight of vehicle %s: %s" % (vehicle_str, multiplier * weight_of_vehicle))
            total_weight += multiplier * weight_of_vehicle
        else:
            # If a match is not found in VEHICLE_TO_WEIGHT dict, just return None
            # print("Weight of %s not found" % vehicle_str)
            return None
    # print("Setting total_weight for %s: %s" % (vehicles_str, total_weight))
    return total_weight


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


def get_vehicle_types_from_raw_data():
    """
    Output of this is currently:
    ['rv', 'van', 'travel trailer', 'car', 'vehicles', 'atv', 'suv', 'pickup', 'other', 'motorcycle',
    'heavy equipment', 'boat']
    :return:
    """
    vehicle_types = set()
    with open('csv_files/raw_listings.csv', 'rb') as raw_csv_file:
        raw_csv_reader = csv.DictReader(raw_csv_file, fieldnames=COLUMNS_TO_SCRAPE)
        for row_dict in raw_csv_reader:
            print("Vehicle types: ", row_dict['vehicles'])
            vehicle_types |= set(raw_vehicles_str_to_vehicle_type_to_count(row_dict['vehicles']).keys())
    print("Vehicle types: ", vehicle_types)


VEHICLE_TO_WEIGHT = {
    'van mini': 4300,
    'van': 4500,
    'van full-size': 5390,

    'motorcycle': 651,

    'convertible': 2979,
    'coupe': 3400,

    'vehicles': 3497,
    'car': 3497,
    'sedan small': 2979,
    's': 3497,
    # 'other': 3497,
    'sedan': 3497,
    'sedan midsize': 3497,
    'sedan large': 4366,

    'suv': 4259,
    'suv small': 3470,
    'suv mid-size': 4259,
    'suv large': 5411,

    'pick': 6000,
    'pickup': 6000,
    'pickup small': 5000,
    'pickup crew cab': 5000,  # ?

    'heavy equipment': 3000,
    'boat': 3000,
    'travel trailer': 3000,
    'atv': 400,
    'rv': 15000  # Really depends on class
}