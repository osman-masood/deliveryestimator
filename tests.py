from listings_scraper import raw_vehicles_str_to_vehicle_type_to_count

if __name__ == "__main__":
    assert raw_vehicles_str_to_vehicle_type_to_count("(Car, Pickup, 3 SUV)") == {'car': 1, 'pickup': 1, 'suv': 3}
    assert raw_vehicles_str_to_vehicle_type_to_count("Car, Pickup, SUV, Car, Car") == {'car': 3, 'pickup': 1, 'suv': 1}
    assert raw_vehicles_str_to_vehicle_type_to_count("Car") == {'car': 1}
