#
# The Blue Dog Application
# Ingest Data Validation Utility
# db_organization_phone.py
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


import datetime
import logging
import traceback
import psycopg2

class DbOrganizationPhone:

    # method: DbOrganizationName()
    # brief: This would be the famous constructor
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def __init__(self):
        logging.debug('Inside: DbOrganizationPhone::DbOrganizationPhone()')

    # method: validation()
    # brief: Validate the data in the row with the database
    # param: row - The row we are going to validate
    # param: age - the age of the house you want
    def validation(self, row, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationPhone::validation()')
        logging.debug("Here is the data")
        logging.debug("Data: EFXID: %s", row['EFXID'])
        logging.debug("Data: EFX_PHONE: %s", row['EFX_PHONE'])
        logging.debug("Data: EFX_FAXPHONE: %s", row['EFX_FAXPHONE'])

        # Set data that we will test
        file_partitionid_in = row['EFXID']
        file_EFX_PHONE_in = row['EFX_PHONE']
        file_EFX_FAXPHONE_in = row['EFX_FAXPHONE']

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
            phonetype_fax = "Headquarters Fax"
            phonetype_phone = "Headquarters Phone"

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_phone where partitionid = '{file_partitionid_in}' AND partition = '{partition_In}' AND phonetype = '{phonetype_phone}'"
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
                db_phone = row[2]
                db_phonetype = row[3]

                logging.debug("db_partition: %s", db_partition)
                logging.debug("db_partitionid: %s", db_partitionid)
                logging.debug("db_phone: %s", db_phone)
                logging.debug("db_phonetype: %s", db_phonetype)

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
            logging.info("Test Class: DbOrganizationPhone")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_phone")
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

            # ------------
            #file_EFX_PHONE_in = file_EFX_PHONE_in.strip()
            db_phone = db_phone.strip()

            try:
                assert str(file_EFX_PHONE_in) == db_phone
                assert_message = f"Test Successful - file_EFX_PHONE_in: {file_EFX_PHONE_in} is equal to db_phone: {db_phone}"
                logging.info(assert_message)
                results.add_to_passed()

            except:
                assert_message = f"Test Failed - file_EFX_PHONE_in: {file_EFX_PHONE_in} Not Equal db_phone: {db_phone}"
                logging.info(assert_message)

        else:
            logging.info("No record were found matching the query")
            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationPhone")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_phone")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition Id: %s", file_partitionid_in)
            results.add_to_no_data()


    # method: validation()
    # brief: Validate the data in the row with the database
    # param: row - The row we are going to validate
    # param: age - the age of the house you want
    def validation_fax(self, row, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationPhone::validation_fax()')
        logging.debug("Here is the data")
        logging.debug("Data: EFXID: %s", row['EFXID'])
        logging.debug("Data: EFX_PHONE: %s", row['EFX_PHONE'])
        logging.debug("Data: EFX_FAXPHONE: %s", row['EFX_FAXPHONE'])

        # Set data that we will test
        file_partitionid_in = row['EFXID']
        file_EFX_PHONE_in = row['EFX_PHONE']
        file_EFX_FAXPHONE_in = row['EFX_FAXPHONE']

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
            phonetype_fax = "Headquarters Fax"
            phonetype_phone = "Headquarters Phone"

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_phone where partitionid = '{file_partitionid_in}' AND partition = '{partition_In}' AND phonetype = '{phonetype_fax}'"
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
                db_phone = row[2]
                db_phonetype = row[3]

                logging.debug("db_partition: %s", db_partition)
                logging.debug("db_partitionid: %s", db_partitionid)
                logging.debug("db_phone: %s", db_phone)
                logging.debug("db_phonetype: %s", db_phonetype)

            logging.info('Close the database connection')
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
            logging.info("Test Class: DbOrganizationPhone")
            logging.info("Test Method: validation_fax()")
            logging.info("Target Database: troa_dev.organization_phone")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition: %s", db_partition)
            logging.info("Partition Id: %s", file_partitionid_in)

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
            # Fax numbers comgin from the Equifax system seem to have a trailing decimal
            # i.e. 6128881234.0
            # We will convert to a integer and then back to a string to compare
            file_EFX_FAXPHONE_in = int(file_EFX_FAXPHONE_in)
            db_phone = db_phone.strip()

            try:
                assert str(file_EFX_FAXPHONE_in) == db_phone
                assert_message = f"Test Successful - file_EFX_FAXPHONE_in: {file_EFX_FAXPHONE_in} is equal to db_phone: {db_phone}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_FAXPHONE_in: {file_EFX_FAXPHONE_in} Not Equal db_phone: {db_phone}"
                logging.info(assert_message)
                results.add_to_failed()
        else:
            logging.info("No record were found matching the query")
            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationPhone")
            logging.info("Test Method: validation_fax()")
            logging.info("Target Database: troa_dev.organization_phone")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition Id: %s", file_partitionid_in)
            results.add_to_no_data()
