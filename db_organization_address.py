#
# The Blue Dog Application
# Ingest Data Validation Utility
# db_organization_addresss.py
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

class DbOrganizationAddress:

    # method: DbOrganizationName()
    # brief: This would be the famous constructor
    # param: name - the name of the house you want
    # param: age - the age of the house you want
    def __init__(self):
        logging.debug('Inside: DbOrganizationAddress::DbOrganizationAddress()')

    # method: validation()
    # brief: Validate the data in the row with the database
    # param: row - The row we are going to validate
    # param: age - the age of the house you want
    def validation(self, row, results):
        logging.debug('------------------------------------------------------------')
        logging.debug('Inside: DbOrganizationAddress::validation()')
        logging.debug("Here is the data")
        logging.debug("Data: EFXID: %s", row['EFXID'])
        logging.debug("Data: EFX_ADDRESS: %s", row['EFX_ADDRESS'])
        logging.debug("Data: EFX_CITY: %s", row['EFX_CITY'])
        logging.debug("Data: EFX_STATE: %s", row['EFX_STATE'])
        logging.debug("Data: EFX_ZIPCODE: %s", row['EFX_ZIPCODE'])
        logging.debug("Data: EFX_ZIP4: %s", row['EFX_ZIP4'])
        logging.debug("Data: EFX_COUNTY: %s", row['EFX_COUNTY'])
        logging.debug("Data: EFX_COUNTYNM: %s", row['EFX_COUNTYNM'])
        logging.debug("Data: EFX_CTRYISOCD: %s", row['EFX_CTRYISOCD'])

        # Set data that we will test
        file_partitionid_in = row['EFXID']
        file_EFX_ADDRESS_in = row['EFX_ADDRESS']
        file_EFX_CITY_in = row['EFX_CITY']
        file_EFX_STATE_in = row['EFX_STATE']
        file_EFX_ZIPCODE_in = row['EFX_ZIPCODE']
        file_EFX_ZIP4_in = row['EFX_ZIP4']
        file_EFX_COUNTY_in = row['EFX_COUNTY']
        file_EFX_COUNTYNM_in = row['EFX_COUNTYNM']
        file_EFX_CTRYISOCD_in = row['EFX_CTRYISOCD']



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

            postgreSQL_select_Query = f"SELECT * FROM troa_dev.organization_address where partitionid = '{file_partitionid_in}' AND partition = '{partition_In}'"
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
                logging.debug("partition = %s", row[0])
                logging.debug("partitionid = %s", row[1])
                logging.debug("addresstype = %s", row[2])
                logging.debug("addressstreet1 = %s", row[3])
                logging.debug("normaddressstreet1 = %s", row[4])
                logging.debug("addressstreet2 = %s", row[5])
                logging.debug("normaddressstreet2 = %s", row[6])
                logging.debug("addressstreet3 = %s", row[7])
                logging.debug("normaddressstreet3 = %s", row[8])
                logging.debug("county = %s", row[13])
                logging.debug("countycode = %s", row[14])
                logging.debug("citycode = %s", row[15])
                logging.debug("city = %s", row[16])
                logging.debug("stateprovidence = %s", row[18])
                logging.debug("postal5 = %s", row[21])
                logging.debug("postal = %s", row[22])
                logging.debug("countrycode = %s", row[23])
                logging.debug("country = %s", row[24])

                # Set the data to compare
                db_partition = row[0]
                db_partitionid = row[1]
                db_addressstreet1 = row[3]
                db_county = row[13]
                db_countycode = row[14]
                db_citycode = row[15]
                db_city = row[16]
                db_stateprovidence = row[18]
                db_postal5 = row[21]
                db_postal = row[22]
                db_countrycode = row[23]
                db_country = row[24]


            logging.info('Close the database connection')
            conn.close()

        except Exception as err:
            logging.info("An exception occurred")
            # print Exception, err

            traceback.print_tb(err.__traceback__)


        # Now we are going to do he data validation asserts
        # this will be fund !!!

        logging.info("************************")
        logging.info("** TEST RESULTS ********")
        logging.info("************************")

        if count > 0:

            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationAddress")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_address")
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
            file_EFX_ADDRESS_in = file_EFX_ADDRESS_in.strip()
            db_addressstreet1 = db_addressstreet1.strip()

            try:
                assert str(file_EFX_ADDRESS_in) == str(db_addressstreet1)
                assert_message = f"Test Successful - file_EFX_ADDRESS_in: {file_EFX_ADDRESS_in} is equal to db_addressstreet1: {db_addressstreet1}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_ADDRESS_in: {file_EFX_ADDRESS_in} Not Equal db_addressstreet1: {db_addressstreet1}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            #file_EFX_COUNTY_in = file_EFX_COUNTY_in.strip()
            # db_county = db_county.strip()

            try:
                assert str(file_EFX_COUNTYNM_in) == str(db_county)
                assert_message = f"Test Successful - file_EFX_COUNTYNM_in: {file_EFX_COUNTYNM_in} is equal to db_county: {db_county}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_COUNTYNM_in: {file_EFX_COUNTYNM_in} Not Equal db_county: {db_county}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            file_EFX_CITY_in = file_EFX_CITY_in.strip()
            db_city = db_city.strip()

            try:
                assert str(file_EFX_CITY_in) == str(db_city)
                assert_message = f"Test Successful - file_EFX_CITY_in: {file_EFX_CITY_in} is equal to db_city: {db_city}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_CITY_in: {file_EFX_CITY_in} Not Equal db_city: {db_city}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            file_EFX_STATE_in = file_EFX_STATE_in.strip()
            db_stateprovidence = db_stateprovidence.strip()

            try:
                assert str(file_EFX_STATE_in) == str(db_stateprovidence)
                assert_message = f"Test Successful - file_EFX_STATE_in: {file_EFX_STATE_in} is equal to db_stateprovidence: {db_stateprovidence}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_STATE_in: {file_EFX_STATE_in} Not Equal db_stateprovidence: {db_stateprovidence}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            #file_EFX_ZIPCODE_in = file_EFX_ZIPCODE_in.strip()
            #db_postal5 = db_postal5.strip()

            if len(db_postal5) > 0:
                db_postal5 = int(db_postal5.strip())

            try:
                assert int(file_EFX_ZIPCODE_in) == db_postal5
                assert_message = f"Test Successful - file_EFX_ZIPCODE_in: {file_EFX_ZIPCODE_in} is equal to db_postal5: {db_postal5}"
                logging.info(assert_message)
                results.add_to_passed()

            except:
                assert_message = f"Test Failed - file_EFX_ZIPCODE_in: {file_EFX_ZIPCODE_in} Not Equal db_postal5: {db_postal5}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            #file_EFX_ZIP4_in = file_EFX_ZIP4_in.strip()
            #db_postal = db_postal.strip()

            try:
                assert str(file_EFX_ZIP4_in) == str(db_postal)
                assert_message = f"Test Successful - file_EFX_ZIP4_in: {file_EFX_ZIP4_in} is equal to db_postal: {db_postal}"
                logging.info(assert_message)
                results.add_to_passed()

            except:
                assert_message = f"Test Failed - file_EFX_ZIP4_in: {file_EFX_ZIP4_in} Not Equal db_postal: {db_postal}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            file_EFX_CTRYISOCD_in = file_EFX_CTRYISOCD_in.strip()
            db_countrycode = db_countrycode.strip()

            try:
                assert str(file_EFX_CTRYISOCD_in) == str(db_countrycode)
                assert_message = f"Test Successful - file_EFX_CTRYISOCD_in: {file_EFX_CTRYISOCD_in} is equal to db_countrycode: {db_countrycode}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_CTRYISOCD_in: {file_EFX_CTRYISOCD_in} Not Equal db_countrycode: {db_countrycode}"
                logging.info(assert_message)
                results.add_to_failed()

            # ------------
            #file_EFX_COUNTY_in = file_EFX_COUNTY_in.strip()

            if len(db_countycode) > 0:
                db_countycode = int(db_countycode.strip())

            try:
                assert file_EFX_COUNTY_in == db_countycode
                assert_message = f"Test Successful - file_EFX_COUNTY_in: {file_EFX_COUNTY_in} is equal to db_countycode: {db_countycode}"
                logging.info(assert_message)
                results.add_to_passed()
            except:
                assert_message = f"Test Failed - file_EFX_COUNTY_in: {file_EFX_COUNTY_in} Not Equal db_countycode: {db_countycode}"
                logging.info(assert_message)
                results.add_to_failed()

        else:
            logging.info("No record were found matching the query")
            current_datetime = datetime.datetime.today()
            logging.info("Test Class: DbOrganizationAddress")
            logging.info("Test Method: validation()")
            logging.info("Target Database: troa_dev.organization_address")
            logging.info("Report Datetime:  %s", current_datetime)
            logging.info("Partition Id: %s", file_partitionid_in)
            results.add_to_no_data()

