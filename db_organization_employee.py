#
# The Blue Dog Application
# Ingest Data Validation Utility
# db_organization_employee.py
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
# When testing the Employee database and the Employee Association Year we have found that our sample did not
# contain any data related to the Employee Association Year. You can use the query below to select data into the
# sample dataset that will contain a Ticker Symbol
#
# SELECT partitionid, associatedyear FROM troa_dev.organization_employee
# where partition = 'Equifax_Domestic' and
# LENGTH(associatedyear) > 0
#
# In other news. It also seems that the associatedyear field in the employee table is not being included in the
# source files. We have found no data in the source or the target database. You can use the query below to
# confirm these findings
#
# SELECT partitionid, associatedyear FROM troa_dev.organization_employee
# where partition = 'Equifax_Domestic' and
# LENGTH(associatedyear) > 0
# ORDER BY partition DESC, partitionid DESC, associatedyear DESC LIMIT 100
#

import datetime
import logging

import traceback
import psycopg2

import math

class DbOrganizationEmployee:

    # method: DbOrganizationTicker()
    # brief: This would be the famous constructor
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def __init__(self):
        logging.debug('Inside: DbOrganizationEmployee::DbOrganizationEmployee()')

    # method: validation()
    # brief: Validate the data in the row with the database
    # param: row - The row we are going to validate
    # param: age - the age of the house you want
    def validation(self, row, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationEmployee::validation()')
        logging.debug("Here is the data")

        # Set data that we will test
        file_partitionid_in = row['EFXID']
        file_EFX_ACTLEMP_in = row['EFX_ACTLEMP']
        file_EFX_ACTLEMPYR_in = row['EFX_ACTLEMPYR']

        logging.debug("Data: EFXID: %s", file_partitionid_in)
        logging.debug("Data: file_EFX_ACTLEMP_in: %s", file_EFX_ACTLEMP_in)
        logging.debug("Data: file_EFX_ACTLEMPYR_in: %s", file_EFX_ACTLEMPYR_in)

        try:

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

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_employee where partitionid = '{file_partitionid_in}' " \
                                      f"AND partition = '{partition_In}' "

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
                db_numemployee = row[2]
                db_associatedyear = row[3]

                logging.debug("partition = %s", db_partition)
                logging.debug("partitionid = %s", db_partitionid)
                logging.debug("db_numemployee = %s", db_numemployee)
                logging.debug("db_associatedyear = %s", db_associatedyear)

            logging.debug('Close the database connection')
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
            logging.info("Test Class: DbOrganizationEmployee")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_employee")
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
            #file_EFX_ACTLEMP_in = file_EFX_ACTLEMP_in.strip()
            # db_numemployee = db_numemployee.strip()

            try:
                assert file_EFX_ACTLEMP_in == db_numemployee
                assert_message = f"Test Successful - file_EFX_ACTLEMP_in: {file_EFX_ACTLEMP_in} is equal to db_numemployee: {db_numemployee}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_ACTLEMP_in: {file_EFX_ACTLEMP_in} Not Equal db_numemployee: {db_numemployee}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            #file_EFX_ACTLEMPYR_in = file_EFX_ACTLEMPYR_in.strip()
            db_associatedyear = db_associatedyear.strip()

            try:
                assert file_EFX_ACTLEMPYR_in == db_associatedyear
                assert_message = f"Test Successful - file_EFX_ACTLEMPYR_in: {file_EFX_ACTLEMPYR_in} is equal to db_associatedyear: {db_associatedyear}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_ACTLEMPYR_in: {file_EFX_ACTLEMPYR_in} Not Equal db_associatedyear: {db_associatedyear}"
                logging.info(assert_message)
                results.add_to_failed()

        else:
            logging.info("No record were found matching the query")
            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationEmployee")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_employee")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition: %s", db_partition)
            logging.info("Partition Id: %s", db_partitionid)
            results.add_to_no_data()
