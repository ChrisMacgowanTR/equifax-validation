
#
# The Blue Dog Application
# Ingest Data Validation Utility
# db_organization_core.py
# Chris Macgowan
# 09 Oct 2019
# We the People
#
# We will use this to validation
# We are testing this here
# have a nice day
#
# Here's how this is going to work
# we will pass
#
# In out sample of data we did not find the tier value
# we have used the query below to find thes data
#
# SELECT partitionid, tier FROM troa_dev.organization_core
# where partition = 'Equifax_Domestic' and tier > 0
# ORDER BY partition DESC, partitionid DESC, tier DESC LIMIT 100
#

import logging

import traceback
import psycopg2

class DbOrganizationCore:

    # method: DbOrganizationCore()
    # brief: This would be the famous constructor
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def __init__(self, name, age, pipe_data):
        logging.debug('Inside: DbOrganizationCore::DbOrganizationCore()')
        self.name = name
        self.age = age
        self.pipe_data = pipe_data

    # method: myfunc()
    # brief: What is the world coming to ???
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def myfunc(self):
        print("Hello my name is " + self.name)

    # method: validation()
    # brief: Validate the data in the row with the database
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def validation(self, row, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationCore::validation()')
        logging.debug("Here is the data")

        # Set data that we will test
        partitionid_in = row['EFXID']
        file_partitionid_in = row['EFXID']
        file_update_date_in = row['EFX_DATE_UPDATED']

        file_EFX_ADDRESS_in = row['EFX_ADDRESS']
        file_EFX_YREST_in = row['EFX_YREST']
        file_EFX_LINKAGE_TIER_in = row['EFX_LINKAGE_TIER']


        logging.debug("Data: partitionid_in: %s", partitionid_in)
        logging.debug("Data: file_partitionid_in: %s", file_partitionid_in)
        logging.debug("Data: file_update_date_in: %s", file_update_date_in)

        logging.debug("Data: file_EFX_ADDRESS_in: %s", file_EFX_ADDRESS_in)
        logging.debug("Data: file_EFX_YREST_in: %s", file_EFX_YREST_in)
        logging.debug("Data: file_EFX_LINKAGE_TIER_in: %s", file_EFX_LINKAGE_TIER_in)


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

            #postgreSQL_select_Query = "SELECT * FROM troa_dev.organization_core where partitionid = '52' AND " \
            #                          "partition = 'Equifax_Domestic' "

            partition_In = "Equifax_Domestic"

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_core where partitionid = '{partitionid_in}' AND partition = '{partition_In}'"

            # stuff_in_string = f"Shepherd {shepherd} is {age} years old."

            logging.debug("SQL string: %s", postgreSQL_select_Query)

            logging.debug("Execute the cursor")
            cursor.execute(postgreSQL_select_Query)

            logging.debug("Selecting rows from mobile table using cursor.fetchall")
            mobile_records = cursor.fetchall()

            count = 0
            for row in mobile_records:
                count += 1

            if count == 0:
                logging.debug("Cursor is empty")

            logging.debug("Row count: %i", count)

            logging.info("Print each row and it's columns values")
            for row in mobile_records:
                logging.debug("partition = %s", row[0])
                logging.debug("partitionid = %s", row[1])
                logging.debug("troaid = %s", row[2])
                logging.debug("bestvalue = %s", row[3])
                logging.debug("cretaedate = %s", row[4])
                logging.debug("modifieddate = %s", row[5])

                # Set the data to compare
                db_partition = row[0]
                db_partitionid = row[1]

                db_update_date_in = row[5]

            logging.debug('Close the database connection')
            conn.close()

        except Exception as err:
            logging.error("An exception occurred")
            # print Exception, err

            traceback.print_tb(err.__traceback__)


        # Now we are going to do he data validation asserts
        # this will be fund !!!

        logging.info("************************")
        logging.info("** TEST RESULTS ********")
        logging.info("************************")

        if count > 0:

            # current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationCore")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_core")
            logging.info("Report Datetime:  %s", "UNKNOWN")
            logging.info("Partition Id: %s", file_partitionid_in)

            #logging.info("String length (file_partitionid_in): %i", len(file_partitionid_in))
            #logging.info("String length (db_partitionid): %i", len(db_partitionid))

            from datetime import datetime, timezone

            t = datetime(2015, 2, 1, 15, 16, 17, 345, tzinfo=timezone.utc)

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

            # file_update_date_in: 10/31/2018
            # db_update_date_in: 2018-10-31 00:00:00+00:00

            file_update_date_in = datetime.strptime(file_update_date_in, '%m/%d/%Y')
            logging.info("The Date 1: %s", file_update_date_in)

            #db_update_date_in = datetime.strptime(db_update_date_in, '%Y-%d/%d')
            #logging.info("The Date 2: %s", db_update_date_in)

            # 2019-05-21 00:00:00+00:00
            datetime_object = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')

            # t1 = datetime(file_update_date_in)
            # t2 = datetime(db_update_date_in)

            try:
                assert file_update_date_in is db_update_date_in
                assert_message = f"Test Successful - file_update_date_in: {file_update_date_in} is equal to db_update_date_in: {db_update_date_in}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_update_date_in: {file_update_date_in} Not Equal db_update_date_in: {db_update_date_in}"
                logging.info(assert_message)
                results.add_to_failed()

        else:
            logging.info("No record were found matching the query")
            # current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationCore")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_core")
            logging.info("Report Datetime:  %s", "UNKNOWN")
            logging.info("Partition Id: %s", file_partitionid_in)
            results.add_to_no_data()






