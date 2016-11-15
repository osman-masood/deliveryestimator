import csv
import time

import requests

CSRF_TOKEN = "c12a9b0cd347de85e49450d0c9f4e156c7234e8dd8102f6f5446b1302dbf00b7"

PAGE_URL = "https://www.centraldispatch.com/protected/listing-search/result?routeBased=0&corridorWidth=&routePickupCity=&routePickupState=&routePickupZip=&route_origination_valid=&routeDeliveryCity=&routeDeliveryState=&routeDeliveryZip=&route_destination_valid=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypoint_valid=1&pickupCity=&pickupRadius=25&pickupState=&pickupZip=&pickupAreas%5B%5D=state_USA_CA&origination_valid=1&deliveryCity=&deliveryRadius=25&deliveryState=&deliveryZip=&deliveryAreas%5B%5D=state_USA_CA&destination_valid=1&FatAllowCanada=1&vehicleTypeIds%5B%5D=&trailerType=&vehiclesRun=&minVehicles=1&maxVehicles=&shipWithin=60&paymentType=&minPayPrice=&minPayPerMile=&highlightOnTop=0&postedBy=&highlightPeriod=0&listingsPerPage=100&primarySort=1&secondarySort=4&filterBlocked=0&highlightPreferred=0&CSRFToken={}".format(
    CSRF_TOKEN)
AJAX_URL = "https://www.centraldispatch.com/protected/listing-search/get-results-ajax?routeBased=0&corridorWidth=&routePickupCity=&routePickupState=&routePickupZip=&route_origination_valid=&routeDeliveryCity=&routeDeliveryState=&routeDeliveryZip=&route_destination_valid=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypoint_valid=1&pickupCity=&pickupRadius=25&pickupState=&pickupZip=&pickupAreas%5B%5D=All&origination_valid=1&deliveryCity=&deliveryRadius=25&deliveryState=&deliveryZip=&deliveryAreas%5B%5D=All&destination_valid=1&FatAllowCanada=1&vehicleTypeIds%5B%5D=&trailerType=&vehiclesRun=&minVehicles=1&maxVehicles=&shipWithin=60&paymentType=&minPayPrice=&minPayPerMile=&highlightOnTop=0&postedBy=&highlightPeriod=0&listingsPerPage=100&primarySort=1&secondarySort=4&filterBlocked=0&highlightPreferred=0&CSRFToken={}&pageStart={}&pageSize=100&template=1"

COLUMNS_TO_SCRAPE = ['listingId', 'formattedCompany', 'comparable',
                     'delivery.state', 'delivery.latitude', 'delivery.longitude',
                     'numVehicles', 'vehiclesCount', 'vehicleOperable', 'vehicles', 'wideLoad',
                     'pickup.state', 'pickup.latitude', 'pickup.longitude',
                     'preferred_shipper', 'price',
                     'rating_count', 'rating_license', 'rating_score']


def listing_value_from_column_name(listing, column_name):
    if '.' in column_name:
        first_col, second_col = column_name.split('.')
        listing_value = listing[first_col].get(second_col, None)
    else:
        listing_value = listing.get(column_name, None)

    # Perform manual type conversions
    if column_name == 'rating_license':
        listing_value = 1 if listing_value == 'valid' else 0
    elif column_name in ('preferred_shipper', 'wideLoad', 'vehicleOperable', 'comparable'):
        listing_value = 1 if listing_value else 0
    elif column_name == 'rating_score':
        listing_value = float(listing_value.replace('%', ''))
    elif column_name == 'vehicles':
        listing_value = listing_value.replace('(', '').replace(')', '')

    return listing_value


if __name__ == "__main__":

    all_listings = []
    has_more_listings = True
    page_start = 0
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
        all_listings += response['listings']
        print("Retrieved {} listings".format(len(response['listings'])))
        page_start += 100
        has_more_listings = len(response['listings']) > 0
        time.sleep(2)

        if page_start > 300:
            break

    print("Retrieved {} listings, writing to alldata.csv...".format(len(all_listings)))
    with open('alldata.csv', 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(COLUMNS_TO_SCRAPE)

        for listing in all_listings:
            row_values = map(lambda column_name: listing_value_from_column_name(listing, column_name),
                             COLUMNS_TO_SCRAPE)
            csv_writer.writerow(row_values)

    print("Done!")
