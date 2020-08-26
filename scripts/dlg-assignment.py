#!/usr/bin/env python
# coding: utf-8

# python C:\temp\dlg\scripts\dlg-assignment.py

from datetime import datetime
from config import config
import common_utils as c_utils

import pandas as pd
import csv

import logging

import glob

import pyarrow as pa
import pyarrow.parquet as pq

import sys

import os
from os import path
from pathlib import Path


# function data_process
def  data_process(logger):
    """
    This function calls 3 functions:
    1. convert_csv_to_parquet()
    2. process_parquet()
    3. results()
    :param name: logger
    :return: status
    """

    try:
        status = convert_csv_to_parquet(logger)
        print("Status (convert_csv_to_parquet) ... {}".format(status))

        if status["Status"] == "Success":
            parquet_df = process_parquet(logger)
            status = results(parquet_df, logger)

    except Exception as e:
        status = {"Status": "Failed"}

        msg = "The data process execution failed with error:{}".format(e.args[0])
        print(msg)
        logger.error(msg)
        raise

    finally:
        return status


# function convert_csv_to_parquet
def  convert_csv_to_parquet(logger):
    """
    This function convert each of csv file into parquet file
    :param name: logger
    :return: status
    """

    try:
        print("\n")

        raw_file_path = config["raw_file_path"]
        msg = "Raw Data Files Path ... : {}".format(raw_file_path)
        print(msg)
        logger.info(msg)

        item_exists = str(path.exists(raw_file_path))
        if item_exists == "False":
            msg = "The given/configured Path {} does not exist ... : {}".format(raw_file_path, item_exists)
            print(msg)
            logger.info(msg)
            status = {"Status": "Failed"}
            return status
        else:
            raw_files_pattern = config["raw_files_pattern"]
            msg = "Raw Data File Names Pattern ... : {}".format(raw_files_pattern)
            print(msg)
            logger.info(msg)
            
            files_to_process = glob.glob(raw_file_path + raw_files_pattern)
            msg = "Files to Process ... : {}".format(files_to_process)
            print(msg)
            logger.info(msg)
    
            # write csv files to parquet format
            li = []
            idx = 0
            for filename in files_to_process:
                msg = "\nProcessing the File Number - {}".format(idx)
                print(msg)
                logger.info(msg)
    
                msg = "File Name ... : {}".format(filename)
                print(msg)
                logger.info(msg)
    
                # Create a Dataframe on data in a file
                df =  pd.read_csv(filename, index_col=None, header=0)
    
                # Convert Dataframe to Apache Arrow Table
                table = pa.Table.from_pandas(df)
    
                # Get/Derive the Parquet file
                parquet_file = filename.lower().replace("raw", "parquet").replace("csv", "parquet")
    
                msg = "Parquet File Name ... : {}".format(parquet_file)
                print(msg)
                logger.info(msg)
    
                # Write Dataframe to Parquet file with GZIP
                msg = "Write to Parquet File Name ... : {}".format(parquet_file)
                print(msg)
                logger.info(msg)
                pq.write_table(table, parquet_file, compression="GZIP")
    
                idx = idx + 1
    
            #if not li:
            if idx == 0:
                msg = "There are no source files to process in the given path ... : {}".format(raw_file_path)
                print(msg)
                logger.info(msg)
                status = {"Status": "Failed"}
                return status

            else:

                status = {"Status": "Success"}
        
                return status

    except Exception as e:
        status = {"Status": "Failed"}

        msg = "The csv to parquet conversion failed with error:{}".format(e.args[0])
        print(msg)
        logger.error(msg)
        raise


# function process_parquet
def  process_parquet(logger):
    """
    This function reads the parquet files into pandas dataframe
    :param name: logger
    :return: parquet_df
    """
    try:
        msg = "\n"
        print(msg)
        logger.info(msg)

        parquet_file_path = config["parquet_file_path"]
        msg = "Parquet Data Files Path ... : {}".format(parquet_file_path)
        print(msg)
        logger.info(msg)
        
        parquet_files_pattern = config["parquet_files_pattern"]
        msg = "Parquet Data File Names Pattern ... : {}".format(parquet_files_pattern)
        print(msg)
        logger.info(msg)
        
        parquet_files_to_process = glob.glob(parquet_file_path + parquet_files_pattern)
        msg = "File Name ... : {}".format(parquet_files_to_process)
        print(msg)
        logger.info(msg)
        
        li = []
        
        for filename in parquet_files_to_process:
            p_df = pd.read_parquet(filename)
            li.append(p_df)
        
        parquet_df = pd.concat(li, axis=0, ignore_index=True, sort=False)

        return parquet_df

    except Exception as e:
        status = {"Status": "Failed"}

        msg = "The parquet file processing failed with error:{}".format(e.args[0])
        print(msg)
        logger.error(msg)

        raise


# function results
def results(parquet_df, logger):
    """
    This function is to print the results
    :param name: parquet_df, logger
    :return: status
    """

    msg = "\n\nPriniting/Logging the results ... : "
    print(msg)
    logger.info(msg)

    try:
        # get the Maximum Temperature value
        max_temp = parquet_df.loc[parquet_df['ScreenTemperature'].idxmax()].loc['ScreenTemperature']
        msg = "The maximum temperature is ... : {}".format(max_temp)
        print(msg)
        logger.info(msg)

        
        # get the row with Maximum Temperature value
        max_temp_date = parquet_df.loc[parquet_df['ScreenTemperature'].idxmax()].loc['ObservationDate']
        msg = "The day with maximum temperature is ... : {}".format(max_temp_date)
        print(msg)
        logger.info(msg)
        
        # get the row with Maximum Temperature value
        max_temp_region = parquet_df.loc[parquet_df['ScreenTemperature'].idxmax()].loc['Region']
        msg = "The region with maximum temperature is ... : {}".format(max_temp_region)
        print(msg)
        logger.info(msg)

        status = {'Status': 'Success'}

        return status

    except Exception as e:
        status = {'Status': 'Failed'}

        msg = "The results display/logging failed with error:{}".format(e.args[0])
        print(msg)
        logger.error(msg)

        raise


# The main() function
def main():
    # Calling data process activity
    job_name = config["data_process_job"]
    logger = c_utils.get_logger(job_name)

    msg = ">>  ==========  Data Processing Job - {} started ... : {}".format(job_name, datetime.now())
    print(msg)
    logger.info(msg)

    # Calling the raw data file processing function
    status = data_process(logger)

    if status == 'Success':
        status = load_to_database()

    msg = "\nThe Final Status of the job - {}  execution is ... : {}".format(job_name, status)
    print(msg)
    logger.info(msg)

    msg = ">>  ==========  Data Processing Job - {} finished ... : {}".format(job_name, datetime.now())
    print(msg)
    logger.info(msg)


if __name__ == '__main__':
    main()
