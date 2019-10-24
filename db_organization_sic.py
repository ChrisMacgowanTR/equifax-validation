#
# The Blue Dog Application
# Ingest Data Validation Utility
# db_organization_sic.py
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

# Notes:
# This class will test the primary and secondar sicid(s)
# the promair validation is implemented
# We need to implement the secondary 1, 2, 3, 4, 5
# We will create a method and then pass in the iteration (1,2,3 ...)
# Then we will be cool ...


import datetime
import logging

import traceback
import psycopg2

class DbOrganizationSic:

    # method: DbOrganizationName()
    # brief: This would be the famous constructor
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def __init__(self):
        logging.debug('Inside: DbOrganizationSic::DbOrganizationSic()')

    # method: validation_primary()
    # brief: Validate the data in the row with the database
    # param: row - The row we are going to validate
    # param: age - the age of the house you want
    def validation_primary(self, row, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationSic::validation_primary()')
        logging.debug("Here is the data")

        # Set data that we will test
        file_partitionid_in = row['EFXID']
        file_EFX_PRIMSIC_in = row['EFX_PRIMSIC']

        logging.debug("Data: EFXID: %s", file_partitionid_in)
        logging.debug("Data: file_EFX_PRIMSIC_in: %s", file_EFX_PRIMSIC_in)

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
            primaryflag = True
            sicorder = 1

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_sic where partitionid = '{file_partitionid_in}' " \
                                      f"AND partition = '{partition_In}' AND primaryflag = '{primaryflag}' AND sicorder = '{sicorder}'"
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
                db_sicid = row[3]

                logging.debug("partition = %s", db_partition)
                logging.debug("partitionid = %s", db_partitionid)
                logging.debug("db_sicid = %s", db_sicid)

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

            current_datetime = datetime.datetime.today()
            logging.info("Partition: %s", db_partition)
            logging.info("Partition Id: %s", db_partitionid)
            logging.info("Report Datetime:  %s", current_datetime)

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
            #file_EFX_PRIMSIC_in = file_EFX_PRIMSIC_in.strip()
            db_sicid = db_sicid.strip()

            try:
                assert file_EFX_PRIMSIC_in == int(db_sicid)
                assert_message = f"Test Successful - file_EFX_PRIMSIC_in: {file_EFX_PRIMSIC_in} is equal to db_sicid: {db_sicid}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_PRIMSIC_in: {file_EFX_PRIMSIC_in} Not Equal db_sicid: {db_sicid}"
                logging.info(assert_message)
                results.add_to_failed()

        else:
            logging.info("No record were found matching the query")
            current_datetime = datetime.datetime.today()
            # logging.info("Partition: %s", db_partition)
            logging.info("Partition Id: %s", file_partitionid_in)
            logging.info("Report Datetime:  %s", current_datetime)
            results.add_to_no_data()


    # method: validation_secondary()
    # brief: Validate the data in the row with the database
    # param: row - The row we are going to validate
    # param: age - the age of the house you want
    def validation_secondary(self, row, sicorder, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationSic::validation_secondary()')
        logging.debug("Here is the data")

        try:

            # Set data that we will test
            file_partitionid_in = row['EFXID']

            if sicorder == 2:
                file_EFX_SECSIC_N_in = row['EFX_SECSIC1']
            elif sicorder == 3:
                file_EFX_SECSIC_N_in = row['EFX_SECSIC2']
            elif sicorder == 4:
                file_EFX_SECSIC_N_in = row['EFX_SECSIC3']
            elif sicorder == 5:
                file_EFX_SECSIC_N_in = row['EFX_SECSIC4']
            elif sicorder == 6:
                file_EFX_SECSIC_N_in = row['EFX_SECSIC5']
            else:
                logging.error("Error: sicorder is not valid. Accepted values are 2-5. sicorder: %d", sicorder)
                raise Exception('A message to the user')

            logging.debug("sicorder: %d", sicorder)
            logging.debug("Data: EFXID: %s", file_partitionid_in)
            logging.debug("Data: file_EFX_SECSIC_N_in: %s", file_EFX_SECSIC_N_in)

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
            primaryflag = False

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_sic where partitionid = '{file_partitionid_in}' " \
                                      f"AND partition = '{partition_In}' AND primaryflag = '{primaryflag}' AND sicorder = '{sicorder}'"
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
                db_sicid = row[3]

                logging.debug("partition = %s", db_partition)
                logging.debug("partitionid = %s", db_partitionid)
                logging.debug("db_sicid = %s", db_sicid)
                logging.debug("db_sicorder = %s", db_sicid)

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
            logging.info("Test Class: DbOrganizationSic")
            logging.info("Test Method: validation_secondary()")
            logging.info("Target Database: troa_dev.organization_sic")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition: %s", db_partition)
            logging.info("Partition Id: %s", db_partitionid)
            logging.info("sicorder:  %d", sicorder)

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
            #file_EFX_PRIMSIC_in = file_EFX_PRIMSIC_in.strip()
            db_sicid = db_sicid.strip()

            try:
                assert file_EFX_SECSIC_N_in == int(db_sicid)
                assert_message = f"Test Successful - file_EFX_SECSIC_N_in: {file_EFX_SECSIC_N_in} is equal to db_sicid: {db_sicid}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_SECSIC_N_in: {file_EFX_SECSIC_N_in} Not Equal db_sicid: {db_sicid}"
                logging.info(assert_message)
                results.add_to_failed()

        else:
            logging.info("No record were found matching the query")
            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationSic")
            logging.info("Test Method: validation_secondary()")
            logging.info("Target Database: troa_dev.organization_sic")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition Id: %s", file_partitionid_in)
            results.add_to_no_data()
