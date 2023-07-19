#!/usr/bin/python3

# the machine where it's running needs to have access to Athena

import csv
import datetime
import json
import logging
import queue
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta

import boto3
import requests
from colorama import Back, Fore, Style

s3 = boto3.resource('s3')

csv.field_size_limit(sys.maxsize)

# pace multiplier
pace = 1

que = queue.Queue()
session = boto3.Session(region_name='eu-west-1')
athena = session.client('athena')

def poll_status(_id):
    result = athena.get_query_execution(
            QueryExecutionId = _id
    )
    state = result['QueryExecution']['Status']['State']
    return state

def feeder():

    athena_results = athena.start_query_execution(
            QueryString = f"""
            SELECT timestamp, originrequest, forwardedrequest FROM \"wbgateway\".\"wbgateway-audit\" WHERE year=2023 AND month=5 AND day=26 AND (forwardedrequest like '%push-framework.webbeds.com:80/request-dotw/%' or originrequest like '%/listeners/SiteMinder.dotw%') ORDER BY timestamp ASC
            """,
        QueryExecutionContext = {
            'Database': 'wbgateway'
        },
        ResultConfiguration = {
            'OutputLocation': 's3://aws-athena-query-results-755621676441-eu-west-1'
        }
    )

    print("Query ID: ", athena_results['QueryExecutionId'])

    query_status = poll_status(athena_results['QueryExecutionId'])

    while query_status != 'SUCCEEDED':
            print("Query it's ", query_status)
            if query_status != 'QUEUED' and query_status != 'RUNNING' and query_status != 'SUCCEEDED':
                    print("Query failed!")
                    return
            time.sleep(3)
            query_status = poll_status(athena_results['QueryExecutionId'])

    # let's bring the results locally... I hate Athena pagination
    if query_status == 'SUCCEEDED':
        print("Query success!")
        s3.Bucket('aws-athena-query-results-755621676441-eu-west-1').download_file(athena_results['QueryExecutionId'] + '.csv', athena_results['QueryExecutionId'])
    else:
        print("Error ocurred, Check process")
        sys.exit()

    with open(athena_results['QueryExecutionId']) as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0

        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
            else:

                row[0], row[1] #... row[n]

