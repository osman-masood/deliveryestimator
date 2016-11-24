from comparables_scraper import comparables_raw_price_to_price_and_miles
from listings_scraper import raw_vehicles_str_to_vehicle_type_to_count

if __name__ == "__main__":
    assert raw_vehicles_str_to_vehicle_type_to_count("(Car, Pickup, 3 SUV)") == {'car': 1, 'pickup': 1, 'suv': 3}
    assert raw_vehicles_str_to_vehicle_type_to_count("Car, Pickup, SUV, Car, Car") == {'car': 3, 'pickup': 1, 'suv': 1}
    assert raw_vehicles_str_to_vehicle_type_to_count("Car") == {'car': 1}

    assert comparables_raw_price_to_price_and_miles("$125($0.74 / mi)") == (125.0, (125.0 / 0.74))
    assert comparables_raw_price_to_price_and_miles("$200 ($3.13/mi)") == (200.0, (200.0 / 3.13))
    assert comparables_raw_price_to_price_and_miles("$1,650 ($0.72/mi)") == (1650.0, (1650.0 / 0.72))
    assert comparables_raw_price_to_price_and_miles('$80 ($13.33/mi)') == (80.0, (80.0 / 13.33))