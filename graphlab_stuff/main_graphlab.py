import logging

from graphlab_estimator import estimate_price, load_symbol_historical_data, set_total_weight_column

logger = logging.getLogger()


def estimate_price_handler(event, context):
    logger.info('Price handler lambda called with: {}'.format(event))

    # Assert that we have the input
    for required_input in ('vehicles', 'in_operation', 'num_vehicles', 'origin', 'destination', 'miles'):
        assert event.get(required_input), "Input field %s is required" % required_input

    # Load the data into SFrame object
    data_sframe = load_symbol_historical_data(filename='CA-2016-11-09.comparable.csv')
    set_total_weight_column(data_sframe)

    # Estimate price
    return estimate_price(data_sframe, vehicles='car', in_operation=True, num_vehicles=1, origin="CA - San Jose, 95136",
                          destination="CA - Sacramento, 95828", miles=128)

