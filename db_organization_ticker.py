#
# The Blue Dog Application
# Ingest Data Validation Utility
# db_organization_ticker.py
# Chris Macgowan
# 20 Oct 2019
# We the People
#
# We will use this to validation
# We are testing this here
# have a nice day
#
# Here's how this is going to work
# we will pass
#
# When testing against a sample of Equifax data we are are only selecting a sample size of 15-20 records.
# typically we will pull from the top, middle and end of the data set in a random and unified manner.
# The Equifax consists of some ~54 million rows - and we are pulling 20, so that is about a
# %0.00003704 sample size.
#
# When testing the Ticker database and the Ticker Symbol we have found that our sample did not contain
# any data related to the Ticker Symbol. You can use the query below to select data into the
# sample dataset that will contain a Ticker Symbol
#
# SELECT partitionid, tickersymbol FROM troa_dev.organization_ticker
# where partition = 'Equifax_Domestic' and
# LENGTH(tickersymbol) > 0
#

import datetime
import logging

import traceback
import psycopg2

import math

class DbOrganizationTicker:

    # method: DbOrganizationTicker()
    # brief: This would be the famous constructor
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def __init__(self):
        logging.debug('Inside: DbOrganizationTicker::DbOrganizationTicker()')

    # method: validation()
    # brief: Validate the data in the row with the database
    # param: row - The row we are going to validate
    # param: age - the age of the house you want
    def validation(self, row, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationTicker::validation()')
        logging.debug("Here is the data")

        # Set data that we will test
        file_partitionid_in = row['EFXID']
        file_EFX_TCKSYM_in = row['EFX_TCKSYM']

        logging.debug("Data: EFXID: %s", file_partitionid_in)
        logging.debug("Data: file_EFX_TCKSYM_in: %s", file_EFX_TCKSYM_in)

        try:

            if isinstance(file_EFX_TCKSYM_in, float):
                logging.error("Note: Ticker Symbol not found in source data")
                raise Exception('A message to the user')

            logging.debug("Attempting to connect o the database")
            conn = psycopg2.connect(dbname='a205718_troa_authority_entity_db_us_east_1_dev',
                                    user='acorn_readwrite_user_dev',
                                    password='y9Tz9D^PWvMM$o*4@!*J',
                                    host='localhost',
                                    port='1234',
                                    sslmode='require')
            logging.debug("Connection was successful")

            cursor = conn.cursor()
            logging.debug("Cursor was created successfully")

            partition_In = "Equifax_Domestic"
            primary_flag = True
            ticker_order = 1

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_ticker where partitionid = '{file_partitionid_in}' " \
                                      f"AND partition = '{partition_In}' AND tickerorder = '{ticker_order}' AND primaryflag = '{primary_flag}'"

            logging.debug("SQL string: %s", postgreSQL_select_Query)

            logging.debug("Execute the cursor")
            cursor.execute(postgreSQL_select_Query)

            logging.debug("Selecting rows from mobile table using cursor.fetchall")
            mobile_records = cursor.fetchall()

            # We will test the row count. We can use this to skip the validation below
            # and report to the user that three is No data

            count = 0
            for row in mobile_records:
                count += 1

            if count == 0:
                logging.debug("Cursor is empty")

            logging.debug("Row count: %i", count)

            logging.debug("Print each row and it's columns values")
            for row in mobile_records:
                # Set the data to compare
                db_partition = row[0]
                db_partitionid = row[1]
                db_tickersymbol = row[2]

                logging.debug("partition = %s", db_partition)
                logging.debug("partitionid = %s", db_partitionid)
                logging.debug("db_tickersymbol = %s", db_tickersymbol)

            logging.info('Close the database connection')
            conn.close()

        except Exception as err:
            logging.error("An exception occurred")
            # print Exception, err
            traceback.print_tb(err.__traceback__)
            results.add_to_exception()
            return
            # sys.exit(100)

        # Now we are going to do he data validation asserts
        # this will be fund !!!

        logging.info("************************")
        logging.info("** TEST RESULTS ********")
        logging.info("************************")

        if count > 0:

            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationTicker")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_ticker")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition: %s", db_partition)
            logging.info("Partition Id: %s", db_partitionid)

            # ------------
            try:
                assert int(file_partitionid_in) == int(db_partitionid)
                assert_message = f"Test Successful - file_partitionid_in: {file_partitionid_in} is equal to db_partitionid: {db_partitionid}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_partitionid_in: {file_partitionid_in} Not Equal db_partitionid: {db_partitionid}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            #file_EFX_TCKSYM_in = file_EFX_TCKSYM_in.strip()
            db_tickersymbol = db_tickersymbol.strip()

            try:
                assert file_EFX_TCKSYM_in == db_tickersymbol
                assert_message = f"Test Successful - file_EFX_TCKSYM_in: {file_EFX_TCKSYM_in} is equal to db_tickersymbol: {db_tickersymbol}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_TCKSYM_in: {file_EFX_TCKSYM_in} Not Equal db_tickersymbol: {db_tickersymbol}"
                logging.info(assert_message)
                results.add_to_failed()

        else:
            logging.info("No record were found matching the query")
            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationTicker")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_ticker")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition: %s", db_partition)
            logging.info("Partition Id: %s", db_partitionid)
            results.add_to_no_data()
