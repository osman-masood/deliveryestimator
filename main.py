import argparse

import boto3

from analysis import analyze_csv
from comparables_scraper import crawl_comparables_from_cleaned_data, clean_comparables_csv, \
    combine_cleaned_comparables_and_listings
from listings_scraper import clean_raw_data
from model_generator import S3_BUCKET, create_aml_model_with_cleaned_data

parser = argparse.ArgumentParser(description='Create an AWS Lambda Deployment')

parser.add_argument('--clean_raw_data', action='store_true')
parser.add_argument('--upload_cleaned_data', action='store_true')
parser.add_argument('--create_aml_model', action='store_true')
parser.add_argument('--analyze_raw_data', action='store_true')
parser.add_argument('--crawl_comparables', action='store_true')
parser.add_argument('--clean_comparables', action='store_true')
parser.add_argument('--combine_cleaned_comparables_and_listings', action='store_true')
args = parser.parse_args()

if args.clean_raw_data:
    clean_raw_data()

if args.upload_cleaned_data:
    print("Uploading file cleaned_listings.csv to S3 into bucket ", S3_BUCKET)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)
    bucket.upload_file("csv_files/cleaned_listings.csv", "cleaned_listings.csv")

if args.create_aml_model:
    create_aml_model_with_cleaned_data(
        included_columns=['listingId', 'truckMiles', 'price_per_mile', 'numVehicles'],
                          # 'rv', 'van', 'travel trailer', 'car', 'atv', 'suv', 'pickup',
                          # 'motorcycle', 'heavy equipment', 'boat'],
        target_field_name='price_per_mile')

if args.analyze_raw_data:
    analyze_csv()

if args.crawl_comparables:
    crawl_comparables_from_cleaned_data()

if args.clean_comparables:
    clean_comparables_csv()

if args.combine_cleaned_comparables_and_listings:
    combine_cleaned_comparables_and_listings()
