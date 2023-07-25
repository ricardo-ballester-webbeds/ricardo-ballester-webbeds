#!/usr/bin/env python3
# ./hcn_derbysoft.py -v

import awswrangler as wr
import argparse 
import pandas as pd 
import boto3
import time
from datetime import datetime, timedelta


from datalake.etl import (
    s3,
    schema,
    database
)

AWS_ATHENA_DB           = "webbeds_searches"
AWS_ATHENA_REGION       = "eu-west-1"
AWS_ATHENA_TMP_RESULTS  = "aws-athena-query-results-137693930675-eu-west-1/scn/"

MAX_RETRIES             = 2

DEFAULT_CONFIG_FILE     = '/home/ec2-user/scripts/scn-etl-jobs/wbconnect-alerts-system/config.json'



# ################################ 
# Static Mappings
# ################################ 





# function to obtain final dataframe structure
def get_df(response) -> pd.DataFrame:
    if response is None:
        return pd.DataFrame()

    df=response

    if df.shape[0] == 0:
        return pd.DataFrame()

    # Convert all columns to lowercase
    df.columns = df.columns.str.lower()

    # Adding exported and request type column
    
    df['request_type'] = args.requesttype
    df['exported'] = pd.to_datetime("now")
    
    return df
            

def get_booking_reference(args) -> pd.DataFrame:
    

    #geting combinations options
    df_sql = pd.DataFrame()
    df = pd.DataFrame()

    
    current_date = args.fromdate
    minus_date = current_date + timedelta(minutes=-args.minutes)

    if args.minutes==0:
        minus_date=datetime.strptime(f'{args.fromdate:%Y-%m-%d} 00:00:00', '%Y-%m-%d %H:%M:%S')
        current_date=datetime.strptime(f'{args.fromdate:%Y-%m-%d} 23:59:59', '%Y-%m-%d %H:%M:%S')

    xmlsuppliername='supplier_name'
    if args.connector is not None and xmlsuppliername != args.connector:
        xmlsuppliername=f"'{args.connector}'"
        
        
    while minus_date <= args.todate:
        
        if args.verbose:
            print(f"  Processing date: from {minus_date} to {current_date}")
        
        requesttype=args.requesttype
        #query to athenas db
        sql = f"""with suppliers_stats as
        (
        select supplier_name, count(1) as total_requests, sum(case when error_code <> '' then 1 else 0 end) as error_requests
        from webbeds_searches.wbconnect_critical
        where year = year(now())
            and month = month(now())
            and day = day(now())
            and hour = hour(now())
            and supplier_name = {xmlsuppliername}
            and start > DATE_ADD('minute', -{args.minutes}, now())
            and request_type = '{requesttype}'
        group by supplier_name
        )
        select supplier_name,total_requests,error_requests,CAST((error_requests/total_requests)*100 AS DOUBLE) as error_ratio
        from suppliers_stats"""

        if args.debug:
            print(sql)

        #save query from athena to dataframe
        df_sql = wr.athena.read_sql_query(
            sql=sql,
            database=AWS_ATHENA_DB,
            ctas_approach=False,
            max_cache_seconds=3600,
        )
        
        df_opt = get_df(df_sql)
        

        df = pd.concat([df, df_opt], ignore_index=True)          
            
        #time.sleep(args.sleep)
        current_date += timedelta(days=1)
        minus_date += timedelta(days=1)

        if args.verbose:
            print(f"  Downloaded {df_sql.shape[0]} suppliers.")
    
    if args.debug:
        print(xmlsuppliername, requesttype)
        
            
    return df



def main(args):
    if args.profile is not None:
        session = boto3.session.Session(profile_name=args.profile)
    else:
        session = boto3.session.Session()

 

    #retrieve request from data_lake searches and critical tables
    df = get_booking_reference(args)
    pd.set_option('max_rows', None) 
    print(df)
    pd.reset_option('max_rows')
    
    if df.empty:
        return
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-v', '--verbose',
        help="Verbose mode",
        action='store_true'
    )

    parser.add_argument(
        '-d', '--debug',
        help="Debug mode",
        action='store_true'
    )

    parser.add_argument(
        '--dry',
        help="Dry mode",
        action='store_true'
    )

    parser.add_argument(
        '-p','--profile',
        help="AWS Profile name",
        required=False
    )

    parser.add_argument(
        '-c', '--connector',
        help="Connector code",
        required=False,
        
    )

    parser.add_argument(
        '-cf', '--connectorfeed',
        help="Connector feed code",
        required=False
    )

    parser.add_argument(
        '-rt', '--requesttype',
        type=str,
        help="Request type. Valid values: search, cancel, prebook, book. Default=book",
        required=True
    )

    parser.add_argument(
        '--fromdays',
        type=int,
        help="Number of days to build the from date since today.",
        required=True
    )

    parser.add_argument(
        '--todays',
        type=int,
        help="Number of days to build the to date since today.",
        required=True
    )


    parser.add_argument(
        '--minutes',
        type=int,
        help="Range of time in minutes for every SQL query",
        required=True
    )

    parser.add_argument(
        '--config',
        help="Config file path.",
        required=False,
        default=DEFAULT_CONFIG_FILE
    )

    args = parser.parse_args()

    if args.debug:
        args.verbose = True

    if args.fromdays:
        args.fromdate = datetime.utcnow() + timedelta(days=args.fromdays)
    
    if args.todays:
        args.todate = datetime.utcnow() + timedelta(days=args.todays)

    main(args)