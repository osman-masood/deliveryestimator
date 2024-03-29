import graphlab

from listings_scraper import convert_vehicles_str_to_weight

"""
Graphlab license:
osman@berkeley.edu
8AF4-0201-9286-9731-83EA-4F5E-4DC7-94DC
"""


def get_unique_vehicles(data_sframe):
    """
    :rtype: set[str]
    """
    vehicle_set = set()
    for vehicle_str in data_sframe['vehicles'].unique():
        vehicles_in_str = vehicle_str.split(',')
        for single_vehicle_str in vehicles_in_str:
            vehicle_set.add(single_vehicle_str)
    return vehicle_set


def set_total_weight_column(data_sframe):
    data_sframe['total_weight'] = data_sframe['vehicles'].apply(convert_vehicles_str_to_weight)


def save_csv_data(filename='CA-2016-11-09.comparable.csv'):
    """
    :rtype: graphlab.SFrame
    """
    print("Saving historical data...")

    data_sframe = graphlab.SFrame.read_csv(filename,
                                           usecols=['vehicles', 'in_operation', 'num_vehicles', 'origin',
                                                    'destination', 'price', 'permile'],
                                           column_type_hints={
                                               'vehicles': str,
                                               'in_operation': str,
                                               'num_vehicles': int,
                                               'origin': str,
                                               'destination': str,
                                               'price': float,
                                               'permile': float
                                           })

    # Convert 'in_operation' to boolean
    data_sframe['in_operation'] = data_sframe['in_operation'].apply(lambda x: x == "YES")

    # Convert 'permile' to 'miles' and remove it
    data_sframe['miles'] = data_sframe['price'] / data_sframe['permile']
    data_sframe.remove_column('permile')

    # Just sanity check data
    print("Top 3 data in %s: %s" % (filename, data_sframe.head(3)))

    # Now we can save data to disk with the SFrame method save, as follows
    data_sframe.save('sframes/%s' % filename)
    return data_sframe


def load_symbol_historical_data(filename='CA-2016-11-09.comparable.csv'):
    """
    :rtype: graphlab.SFrame
    """
    print("Loading historical data from: %s" % filename)
    return graphlab.SFrame('sframes/%s' % filename)


def create_training_and_testing_sframes(sframe_obj, testing_set_ratio_or_count=0.2):
    if testing_set_ratio_or_count <= 1.0:
        # It is a ratio
        training_set_ratio = 1 - testing_set_ratio_or_count
        training = sframe_obj[0: round(len(sframe_obj) * training_set_ratio)]  # type: graphlab.SFrame
        testing = sframe_obj[round(len(sframe_obj) * training_set_ratio):]  # type: graphlab.SFrame
    else:
        # It is a count
        training = sframe_obj[0: -testing_set_ratio_or_count]  # type: graphlab.SFrame
        testing = sframe_obj[-testing_set_ratio_or_count:]  # type: graphlab.SFrame

    return training, testing


def test_models(sframe_obj):
    """

    :param graphlab.SFrame sframe_obj:
    :return:
    """

    # Create training and testing sets
    training, testing = create_training_and_testing_sframes(sframe_obj, testing_set_ratio_or_count=30)

    # Linear regression model
    # features = ['total_weight', 'in_operation', 'num_vehicles', 'miles']
    features = None

    # Perform generic regression analysis
    model = graphlab.regression.create(training.dropna(columns=['total_weight']), target='price',
                                       features=features, validation_set=None, verbose=False)
    testing['predictions'] = model.predict(testing)
    print("Prediction with generic regression model:\n")
    testing[['total_weight', 'num_vehicles', 'miles', 'price', 'predictions']].print_rows(num_rows=10)
    evaluation = model.evaluate(testing)
    print("Evaluation with generic regression model:\n{}".format(evaluation))

    # Perform linear regression analysis
    model = add_predictions_with_linear_regression_model(training, testing, features)
    print("Prediction with linear regression model:\n")
    testing[['total_weight', 'num_vehicles', 'miles', 'price', 'predictions']].print_rows(num_rows=10)
    evaluation = model.evaluate(testing)
    print("Evaluation with linear regression model:\n{}".format(evaluation))

    # Perform decision tree regression analysis
    model = add_predictions_with_decision_tree_regression_model(training, testing, features=features)
    print("Prediction with decision tree regression model:\n")
    testing[['total_weight', 'num_vehicles', 'miles', 'price', 'predictions']].print_rows(num_rows=10)
    evaluation = model.evaluate(testing)
    print("Evaluation with decision tree regression model:\n{}".format(evaluation))

    # Perform boosted tree regression analysis
    model = add_predictions_with_boosted_trees_regression_model(training, testing, features=features)
    print("Prediction with boosted tree regression model:\n")
    testing[['total_weight', 'num_vehicles', 'miles', 'price', 'predictions']].print_rows(num_rows=10)
    evaluation = model.evaluate(testing)
    print("Evaluation with boosted tree regression model:\n{}".format(evaluation))

    # Perform random forest regression analysis
    model = add_predictions_with_random_forest_regression_model(training, testing, features=features)
    print("Prediction with random forest regression model:\n")
    testing[['total_weight', 'num_vehicles', 'miles', 'price', 'predictions']].print_rows(num_rows=10)
    evaluation = model.evaluate(testing)
    print("Evaluation with random forest regression model:\n{}".format(evaluation))


def add_predictions_with_linear_regression_model(training, testing, l_lr_features=None):
    model = graphlab.linear_regression.create(training.dropna(columns=['total_weight']), target='price',
                                              features=l_lr_features, validation_set=None,
                                              verbose=False, max_iterations=5000)
    predictions = model.predict(testing)
    testing['predictions'] = predictions
    return model


def add_predictions_with_decision_tree_regression_model(training, testing, features=None):
    model = graphlab.decision_tree_regression.create(dataset=training, target='price', features=features,
                                                     validation_set=None, max_depth=10)
    predictions = model.predict(testing)
    testing['predictions'] = predictions
    return model


def add_predictions_with_boosted_trees_regression_model(training, testing, features=None):
    model = graphlab.boosted_trees_regression.create(dataset=training, target='price', features=features,
                                                     validation_set=None, max_iterations=10, max_depth=10)
    predictions = model.predict(testing)
    testing['predictions'] = predictions
    return model


def add_predictions_with_random_forest_regression_model(training, testing, features=None):
    model = graphlab.random_forest_regression.create(dataset=training, target='price', features=features,
                                                     validation_set=None, max_iterations=10)
    predictions = model.predict(testing)
    testing['predictions'] = predictions
    return model


def estimate_price(training, vehicles, in_operation, num_vehicles, origin, destination, miles):
    """
    Given training data and other params, return estimated price

    :param graphlab.SFrame training:
    :param str vehicles:
    :param bool in_operation:
    :param int num_vehicles:
    :param str origin:
    :param str destination:
    :param int miles:
    :rtype: float
    """

    # Create training and testing sets
    training, testing = create_training_and_testing_sframes(training, testing_set_ratio_or_count=0)

    input_sframe = graphlab.SFrame({'vehicles': [vehicles],
                                    'in_operation': [in_operation],
                                    'num_vehicles': [num_vehicles],
                                    'origin': [origin],
                                    'destination': [destination],
                                    'miles': [miles],
                                    'price': [0]})

    # Perform boosted tree regression analysis
    # features = ['total_weight', 'in_operation', 'num_vehicles', 'miles']
    add_predictions_with_boosted_trees_regression_model(training, input_sframe)
    print("Prediction: {}".format(input_sframe['predictions']))
    return {"prediction": input_sframe['predictions']}

if __name__ == "__main__":
    # data_sframe = save_csv_data()

    data_sframe = load_symbol_historical_data(filename='CA-2016-11-09.comparable.csv')

    # print "Unique vehicles: %s" % (get_unique_vehicles(data_sframe))

    set_total_weight_column(data_sframe)

    data_sframe.save('sframes/CA-2016-11-09.final.csv', format='csv')

    # test_models(data_sframe)

    # estimate_price(data_sframe, vehicles='car', in_operation=True, num_vehicles=1, origin="CA - San Jose, 95136",
    #                destination="CA - Sacramento, 95828", miles=128)
