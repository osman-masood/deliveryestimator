import csv
from pprint import pprint

import numpy

from listings_scraper import get_cleaned_listings_column_groups


def analyze_csv():
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

    # TODO look at average price per mile in each state. Does it differ?

    col_name_to_value_to_count = {
        # 'comparable': {},
        'numVehicles': {},
        'vehicleOperable': {},
        'vehicles': {},
        # 'vehicle_types': {},
        # 'vehicle_make': {},
        'truckMiles': {},
        'price': {}
    }  # Maps field name to value to # of records w/ that value

    # For every 'vehicles' type, get the average price/mile, along with standard deviation
    vehicles_and_states_to_ppm_ratios = {}

    columns_from_raw_to_write, vehicle_types, computed_fields = get_cleaned_listings_column_groups()
    columns_to_write = columns_from_raw_to_write + vehicle_types + computed_fields

    with open('csv_files/cleaned_combined.csv', 'rb') as csvfile:
        csv_reader = csv.DictReader(csvfile, fieldnames=columns_to_write)
        for row in csv_reader:
            for col_name in col_name_to_value_to_count.keys():
                col_name_to_value_to_count[col_name].setdefault(row[col_name], 0)
                col_name_to_value_to_count[col_name][row[col_name]] += 1

            # Add vehicle to ppm ratio
            def append_delivery_and_pickup_states(row_obj):
                return '_'.join(sorted([row_obj['delivery.state'], row_obj['pickup.state']]))
            try:
                key = "{}_{}".format(row['vehicles'], append_delivery_and_pickup_states(row))
                vehicles_and_states_to_ppm_ratios.setdefault(key, [])
                vehicles_and_states_to_ppm_ratios[key].append(float(row['price']) / float(row['truckMiles']))
            except ValueError:  # "could not convert string to float: price"
                pass

    print("col_name_to_value_to_count: \n")
    pprint(col_name_to_value_to_count)

    # For each 'vehicles' type, print avg and std dev
    for vehicles_type_and_states in vehicles_and_states_to_ppm_ratios:
        ppm_ratios = vehicles_and_states_to_ppm_ratios[vehicles_type_and_states]
        if len(ppm_ratios) > 10:  # Ignore superfluous entries
            print("{}: PPM ratio len: {}, average: {}, standard dev: {}".format(vehicles_type_and_states, len(ppm_ratios),
                                                                                numpy.mean(ppm_ratios),
                                                                                numpy.std(ppm_ratios)))