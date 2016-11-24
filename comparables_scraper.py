import csv
import re
import time
from random import randint

import requests
from bs4 import BeautifulSoup

from listings_scraper import CSRF_TOKEN, get_cleaned_listings_column_groups, raw_vehicles_str_to_vehicle_type_to_count

COMPARABLES_RAW_COLUMNS = ['listingId', 'cargo', 'route', 'price', 'accepted_by_carrier']

COMPARABLES_CLEANED_COLUMNS = ['listingId', 'vehicles', 'vehicleOperable', 'numVehicles', 'pickup.city', 'pickup.state',
                               'delivery.city', 'delivery.state', 'truckMiles', 'price', 'price_per_mile'] + \
                              ['rv', 'van', 'travel trailer', 'car', 'atv', 'suv', 'pickup', 'other', 'motorcycle',
                               'heavy equipment', 'boat']

COMBINED_CLEANED_COLUMNS = ['vehicles', 'vehicleOperable', 'numVehicles', 'pickup.city', 'pickup.state',
                            'delivery.city', 'delivery.state', 'truckMiles', 'price', 'price_per_mile'] + \
                           ['rv', 'van', 'travel trailer', 'car', 'atv', 'suv', 'pickup', 'other', 'motorcycle',
                            'heavy equipment', 'boat']

PRICES_HTML_EXAMPLE_1 = """
<table class="FatSamples table">
    <thead>
    <tr>
        <th>Cargo</th>
        <th>Route</th>
        <th>Price</th>
        <th class="center">Accepted<br>by Carrier?</th>
        <th class="center">Comparable Price<br>for 2,489mi</th>
    </tr>
    </thead>
    <tr >
        <td>Car                                                           </td>
        <td nowrap>ft walton beach, FL<br/>hillsborough, CA</td>
        <td class="right">$1,000<br/>($0.40/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$996*</td>
    </tr>
    <tr class="even">
        <td>Car                                                           (<span class="red">inop</span>)</td>
        <td nowrap>crestview, FL<br/>sacramento, CA</td>
        <td class="right">$900<br/>($0.36/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$896&nbsp;</td>
    </tr>
    <tr >
        <td>Car                                                           </td>
        <td nowrap>montgomery, AL<br/>north highlands, CA</td>
        <td class="right">$850<br/>($0.35/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$871*</td>
    </tr>
    <tr class="even">
        <td>Car  </td>
        <td nowrap>fort benning, GA<br/>sacramento, CA</td>
        <td class="right">$850<br/>($0.34/mi)</td>
        <td class="center">Unknown</td>
        <td class="right">$846*</td>
    </tr>
    <tr >
        <td>Car  </td>
        <td nowrap>columbus, GA<br/>sacramento, CA</td>
        <td class="right">$701<br/>($0.28/mi)</td>
        <td class="center"><a href='javascript:void(0)' class='showListing' data-id='41543577' ">Still Posted</a></td>
        <td class="right">$697*</td>
    </tr>
                <tfoot>
    <tr><th colspan="5" class="right">* This price does not include premium for inop.</th></tr>
    </tfoot>
</table>

<div id="FatSamplesListing"></div>
"""

PRICES_HTML_EXAMPLE_2 = """
<table class="FatSamples table">
    <thead>
    <tr>
        <th>Cargo</th>
        <th>Route</th>
        <th>Price</th>
        <th class="center">Accepted<br>by Carrier?</th>
        <th class="center">Comparable Price<br>for 1,765mi</th>
    </tr>
    </thead>
    <tr >
        <td>Car                                                           </td>
        <td nowrap>greensboro, AL<br/>las vegas, NV</td>
        <td class="right">$800<br/>($0.45/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$794*</td>
    </tr>
    <tr class="even">
        <td>SUV Small                                                     </td>
        <td nowrap>birmingham, AL<br/>las vegas, NV</td>
        <td class="right">$725<br/>($0.40/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$706*</td>
    </tr>
    <tr >
        <td>Car                                                           </td>
        <td nowrap>tuscaloosa, AL<br/>fort irwin, CA</td>
        <td class="right">$750<br/>($0.39/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$688*</td>
    </tr>
    <tr class="even">
        <td>Car                                                           </td>
        <td nowrap>oxford, AL<br/>las vegas, NV</td>
        <td class="right">$700<br/>($0.37/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$653*</td>
    </tr>
    <tr >
        <td>Car  </td>
        <td nowrap>bessemer, AL<br/>fort mohave, AZ</td>
        <td class="right">$600<br/>($0.34/mi)</td>
        <td class="center"><a href='javascript:void(0)' class='showListing' data-id='41554123' ">Still Posted</a></td>
        <td class="right">$600*</td>
    </tr>
                <tfoot>
    <tr><th colspan="5" class="right">* This price does not include premium for inop.</th></tr>
    </tfoot>
</table>

<div id="FatSamplesListing"></div>
"""

PRICES_HTML_EXAMPLE_3 = """
<table class="FatSamples table">
    <thead>
    <tr>
        <th>Cargo</th>
        <th>Route</th>
        <th>Price</th>
        <th class="center">Accepted<br>by Carrier?</th>
        <th class="center">Comparable Price<br>for 67mi</th>
    </tr>
    </thead>
    <tr >
        <td>Car                                                           </td>
        <td nowrap>tanner, AL<br/>birmingham, AL</td>
        <td class="right">$100<br/>($1.01/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$135&nbsp;</td>
    </tr>
    <tr class="even">
        <td>Van                                                           </td>
        <td nowrap>athens, AL<br/>birmingham, AL</td>
        <td class="right">$100<br/>($0.93/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$125&nbsp;</td>
    </tr>
    <tr >
        <td>Car, Car                                                      </td>
        <td nowrap>cartersville, GA<br/>wenonah, AL</td>
        <td class="right">$200<br/>($0.63/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$84&nbsp;</td>
    </tr>
    <tr class="even">
        <td>Car, Car, Car                                                 </td>
        <td nowrap>cartersville, GA<br/>birmingham, AL</td>
        <td class="right">$300<br/>($0.63/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$84&nbsp;</td>
    </tr>
    <tr >
        <td>Car, Car, Pickup, Pickup, SUV                                 </td>
        <td nowrap>athens, AL<br/>prattville, AL</td>
        <td class="right">$500<br/>($0.53/mi)</td>
        <td class="center">YES <img src="/images/confirm_16.png" width="16" height="16" style="vertical-align: bottom;"></td>
        <td class="right">$71&nbsp;</td>
    </tr>
                <tfoot>
    <tr><th colspan="5" class="right">&nbsp;</th></tr>
    </tfoot>
</table>

<div id="FatSamplesListing"></div>
"""


def crawl_comparables_from_cleaned_data():
    """
    Goes through cleaned_listings.csv, gets everything that has comparable=True, and makes API call to get the data
    :return:
    """

    COMPARABLES_URL = 'https://www.centraldispatch.com/protected/cargo/sample-prices?id={}'
    COMPARABLES_HTTP_REFERER = """https://www.centraldispatch.com/protected/listing-search/result?routeBased=0&corridorWidth=&routePickupCity=&routePickupState=&routePickupZip=&route_origination_valid=&routeDeliveryCity=&routeDeliveryState=&routeDeliveryZip=&route_destination_valid=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointCity%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointState%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypointZip%5B%5D=&waypoint_valid=1&pickupCity=&pickupRadius=25&pickupState=&pickupZip=&pickupAreas%5B%5D=All&origination_valid=1&deliveryCity=&deliveryRadius=25&deliveryState=&deliveryZip=&deliveryAreas%5B%5D=All&destination_valid=1&FatAllowCanada=1&vehicleTypeIds%5B%5D=&trailerType=&vehiclesRun=&minVehicles=1&maxVehicles=&shipWithin=60&paymentType=&minPayPrice=&minPayPerMile=&highlightOnTop=0&postedBy=&highlightPeriod=0&listingsPerPage=500&primarySort=1&secondarySort=4&filterBlocked=0&highlightPreferred=0&CSRFToken={}""".format(
        CSRF_TOKEN)

    # In case this function throws exception: Find where in cleaned_listings.csv we left off (at which listingId)
    final_comparables_listing_id = None
    with open('csv_files/comparables.csv', 'rb') as comparables_handle:
        csv_reader = csv.DictReader(comparables_handle,
                                    fieldnames=COMPARABLES_RAW_COLUMNS)
        for comparables_dict in csv_reader:
            final_comparables_listing_id = comparables_dict['listingId']

    # Now loop through cleaned data, GET comparables for the listingId, and append to comparables file.
    with open('csv_files/cleaned_listings.csv', 'rb') as cleaned_data_handle:
        csv_reader = csv.DictReader(cleaned_data_handle, fieldnames=['listingId', 'formattedCompany', 'comparable'])
        with open('csv_files/comparables.csv', 'a') as comparables_handle:
            csv_writer = csv.DictWriter(comparables_handle, fieldnames=COMPARABLES_RAW_COLUMNS)
            for cleaned_data_dict in csv_reader:

                # If we know the final listing ID, keep iterating until we find that
                if final_comparables_listing_id:
                    if cleaned_data_dict['listingId'] == final_comparables_listing_id:
                        print("Previous listingId reached: ", final_comparables_listing_id)
                        final_comparables_listing_id = None
                    else:
                        print("Skipping row in cleaned_data, since not reached yet")
                        continue  # Skip this row in the cleaned data, since we've already reached it

                if cleaned_data_dict['comparable'].lower() in ('1', 'true'):
                    response = requests.get(COMPARABLES_URL.format(cleaned_data_dict['listingId']),
                                            headers={
                                                "Accept": "application/json, text/javascript, */*; q=0.01",
                                                "Accept-Encoding": "gzip, deflate, sdch, br",
                                                "Accept-Language": "en-US,en;q=0.8,sv;q=0.6",
                                                "Cache-Control": "no-cache",
                                                "Connection": "keep-alive",
                                                "Cookie": "CSRF_TOKEN={}; visitedDashboard=1; defaultView=list; test-persistent=1; test-session=1; PHPSESSID=d9d8aff688df964876b9bf61b4d51c5a".format(
                                                    CSRF_TOKEN),
                                                "Host": "www.centraldispatch.com",
                                                "Pragma": "no-cache",
                                                "Upgrade-Insecure-Requests": "1",
                                                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36",
                                                "Referer": COMPARABLES_HTTP_REFERER,
                                                "X-NewRelic-ID": "VQcFUFBUCxAJUFFSAQQO",
                                                "X-Requested-With": "XMLHttpRequest"
                                            }).json()
                    row_dicts = parse_comparables_prices_html(response['prices'])
                    for row_dict in row_dicts:
                        row_dict['listingId'] = cleaned_data_dict['listingId']
                    csv_writer.writerows(row_dicts)
                    print("Writing comparables: ", row_dicts)
                    time.sleep(10 + randint(5, 10))
    print("Finished writing comparables.cvs")


def parse_comparables_prices_html(price_html):
    results = []  # List of dicts with cargo, route, price, and accepted_by_carrier keys
    soup = BeautifulSoup(price_html, 'lxml')
    tr_tags = soup.find_all("tr")
    for tr_tag in tr_tags:
        td_tags = tr_tag.find_all('td')
        if td_tags:
            print("TD tags: ", td_tags)
            results.append({
                'cargo': td_tags[0].get_text(" ", strip=True),
                'route': td_tags[1].get_text(" ", strip=True),
                'price': td_tags[2].get_text(" ", strip=True),
                'accepted_by_carrier': td_tags[3].get_text(" ", strip=True)
            })
    return results


if __name__ == "__main__":
    crawl_comparables_from_cleaned_data()

    # pprint(parse_price_html(PRICES_HTML_EXAMPLE_1))
    # pprint(parse_price_html(PRICES_HTML_EXAMPLE_2))
    # pprint(parse_price_html(PRICES_HTML_EXAMPLE_3))


def comparables_raw_price_to_price_and_miles(price_str):
    matchings = re.match(r"\$([\d.,]+)\s*"  # "$125 "
                         r"\(\$([\d.]+)"  # "($0.74"
                         r"\s*/\s*mi\)",  # " / mi)"
                         price_str, re.IGNORECASE)
    if not matchings.groups():
        return None
    price, price_per_mile = matchings.groups()
    price = float(price.replace(',', '').strip())
    miles = float(price / float(price_per_mile.strip()))
    return price, miles


def clean_comparables_csv():
    columns_from_raw_to_write, vehicle_types, computed_fields = get_cleaned_listings_column_groups()

    with open('csv_files/comparables.csv', 'rb') as comparables_csv_handle:
        csv_reader = csv.DictReader(comparables_csv_handle, fieldnames=COMPARABLES_RAW_COLUMNS)
        with open('csv_files/cleaned_comparables.csv', 'wb') as comparables_cleaned_csv_handle:
            csv_writer = csv.DictWriter(comparables_cleaned_csv_handle, fieldnames=COMPARABLES_CLEANED_COLUMNS)
            csv_writer.writeheader()
            for raw_comparables_dict in csv_reader:
                cleaned_comparables_dict = {'listingId': raw_comparables_dict['listingId']}

                # Get vehicleOperable and vehicles columns from 'cargo'
                raw_vehicles_str = raw_comparables_dict['cargo']
                cleaned_comparables_dict['vehicleOperable'] = "inop" in raw_vehicles_str.lower()
                cleaned_comparables_dict['vehicles'] = re.sub(r"\(\s+inop\s+\)", "", raw_vehicles_str,
                                                              flags=re.IGNORECASE).strip()

                # Strip off '( encl )' which is rarely there
                cleaned_comparables_dict['vehicles'] = re.sub(r"\(\s+encl\s+\)", "",
                                                              cleaned_comparables_dict['vehicles'],
                                                              flags=re.IGNORECASE).strip()

                # Now from 'vehicles', set car type counts
                vehicle_type_to_count = raw_vehicles_str_to_vehicle_type_to_count(cleaned_comparables_dict['vehicles'])
                for vehicle_type in vehicle_types:
                    cleaned_comparables_dict[vehicle_type] = vehicle_type_to_count.get(vehicle_type, 0)

                # Compute numVehicles from above
                cleaned_comparables_dict['numVehicles'] = \
                    sum([cleaned_comparables_dict[vehicle_type] for vehicle_type in vehicle_types])

                # Get pickup & destination city/state from 'route'
                route_matchings = re.match(r"(.*),\s*(\w{2})\s+(.*),\s+(\w{2})", raw_comparables_dict['route']).groups()
                if len(route_matchings) != 4:
                    raise Exception("4 route matchings not found in: {}".format(raw_comparables_dict))
                (cleaned_comparables_dict['pickup.city'], cleaned_comparables_dict['pickup.state'],
                 cleaned_comparables_dict['delivery.city'], cleaned_comparables_dict['delivery.state']) = \
                    route_matchings

                # Get price, price_per_mile, and truckMiles from 'price'
                cleaned_comparables_dict['price'], cleaned_comparables_dict['truckMiles'] = \
                    comparables_raw_price_to_price_and_miles(raw_comparables_dict['price'])
                cleaned_comparables_dict['price_per_mile'] = cleaned_comparables_dict['price'] / float(
                    cleaned_comparables_dict['truckMiles'])

                if cleaned_comparables_dict['truckMiles'] < 10.0 or cleaned_comparables_dict['price'] < 10.0:
                    print("Invalid raw comparables row, miles or price too low: ", raw_comparables_dict)
                    continue

                # Write it
                csv_writer.writerow(cleaned_comparables_dict)


def combine_cleaned_comparables_and_listings():
    """
    Combines results from cleaned_listings.csv and cleaned_comparables.csv. Doesn't include listingId
    :return:
    """

    # Post in comparables data.
    with open('csv_files/cleaned_combined.csv', 'wb') as cleaned_combined_handle:
        combined_writer = csv.DictWriter(cleaned_combined_handle, fieldnames=COMBINED_CLEANED_COLUMNS)
        combined_writer.writeheader()

        with open('csv_files/cleaned_comparables.csv', 'rb') as comparables_cleaned_csv_handle:
            comparables_reader = csv.DictReader(comparables_cleaned_csv_handle, fieldnames=COMPARABLES_CLEANED_COLUMNS)
            i = 0
            comparables_reader.next()  # Skip header row
            for i, comparables_dict in enumerate(comparables_reader):
                combined_dict = {col_name: comparables_dict[col_name] for col_name in COMBINED_CLEANED_COLUMNS}
                combined_writer.writerow(combined_dict)
            print("Write {} rows of from cleaned_comparables.csv".format(i + 1))

        with open('csv_files/cleaned_listings.csv', 'rb') as listings_cleaned_csv_handle:
            columns_from_raw_to_write, vehicle_types, computed_fields = get_cleaned_listings_column_groups()
            cleaned_listings_columns = columns_from_raw_to_write + vehicle_types + computed_fields

            listings_reader = csv.DictReader(listings_cleaned_csv_handle, fieldnames=cleaned_listings_columns)
            listings_reader.next()  # Skip header row
            i = 0
            for i, listings_dict in enumerate(listings_reader):
                combined_dict = {col_name: listings_dict[col_name] for col_name in COMBINED_CLEANED_COLUMNS}
                combined_writer.writerow(combined_dict)
            print("Write {} rows of from cleaned_listings.csv".format(i + 1))
