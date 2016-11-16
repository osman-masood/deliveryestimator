import csv
import time
from pprint import pprint

import requests

from estimator import convert_vehicles_str_to_weight

CSRF_TOKEN = "c12a9b0cd347de85e49450d0c9f4e156c7234e8dd8102f6f5446b1302dbf00b7"

PAGE_URL = "https://www.centraldispatch.com/protected/listing-search/result?routeBased=0&corridorWidth=&routePickupCity=&routePickupState=&routePickupZip=&route_origination_valid=&routeDeliveryCity=&routeDeliveryState=&routeDeliveryZip=&route_destination_valid=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypoint_valid=1&pickupCity=&pickupRadius=25&pickupState=&pickupZip=&pickupAreas%5B%5D=state_USA_CA&origination_valid=1&deliveryCity=&deliveryRadius=25&deliveryState=&deliveryZip=&deliveryAreas%5B%5D=state_USA_CA&destination_valid=1&FatAllowCanada=1&vehicleTypeIds%5B%5D=&trailerType=&vehiclesRun=&minVehicles=1&maxVehicles=&shipWithin=60&paymentType=&minPayPrice=&minPayPerMile=&highlightOnTop=0&postedBy=&highlightPeriod=0&listingsPerPage=500&primarySort=1&secondarySort=4&filterBlocked=0&highlightPreferred=0&CSRFToken={}".format(
    CSRF_TOKEN)
AJAX_URL = "https://www.centraldispatch.com/protected/listing-search/get-results-ajax?routeBased=0&corridorWidth=&routePickupCity=&routePickupState=&routePickupZip=&route_origination_valid=&routeDeliveryCity=&routeDeliveryState=&routeDeliveryZip=&route_destination_valid=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypoint_valid=1&pickupCity=&pickupRadius=25&pickupState=&pickupZip=&pickupAreas%5B%5D=All&origination_valid=1&deliveryCity=&deliveryRadius=25&deliveryState=&deliveryZip=&deliveryAreas%5B%5D=All&destination_valid=1&FatAllowCanada=1&vehicleTypeIds%5B%5D=&trailerType=&vehiclesRun=&minVehicles=1&maxVehicles=&shipWithin=60&paymentType=&minPayPrice=&minPayPerMile=&highlightOnTop=0&postedBy=&highlightPeriod=0&listingsPerPage=500&primarySort=1&secondarySort=4&filterBlocked=0&highlightPreferred=0&CSRFToken={}&pageStart={}&pageSize=100&template=1"

COLUMNS_TO_SCRAPE = ['listingId', 'formattedCompany', 'comparable',
                     'delivery.state', 'delivery.latitude', 'delivery.longitude', 'delivery.city',
                     'numVehicles', 'vehiclesCount', 'vehicleOperable', 'vehicles', 'vehicle_make', 'vehicle_types',
                     # Use numVehicles instead of vehiclesCount
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
        while has_more_listings:
            print("AJAX call with page_start={}".format(page_start))
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
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(COLUMNS_TO_SCRAPE)
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


def analyze_raw_data_csv():
    """
    Analyzes alldata.csv (output of scraping CentralDispatch) and prints results
    """
    col_name_to_value_to_count = {
        'comparable': {},
        'numVehicles': {},
        'preferred_shipper': {},
        'vehicleOperable': {},
        'vehicles': {},
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


if __name__ == "__main__":
    # scrape_central_dispatch()
    analyze_raw_data_csv()

