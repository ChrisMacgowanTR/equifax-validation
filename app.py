#
# Acorn Ingest Validation
# Equifax Validation
# Chris Macgowan
# 09 Oct 2019
# app.py
#
# This is add will help with ingest data validation. It will be used to read the
# input file that was used during the ingest process. We will parse the input
# and then we will compare the data to the data in the database.
#
# When testing against a sample of Equifax data we are are only selecting a sample size of 15-20 records.
# typically we will pull from the top, middle and end of the data set in a random and unified manner.
# The Equifax consists of some ~54 million rows - and we are pulling 20, so that is about a
# %0.00003704 sample size.
#

import logging
import sys
import traceback
import psycopg2

import results as module_results

import db_organization_core as module_db_organization_core
import db_organization_name as module_db_organization_name
import db_organization_address as module_db_organization_address
import db_organization_phone as module_db_organization_phone
import db_organization_email as module_db_organization_email
import db_organization_website as module_db_organization_website
import db_organization_sic as module_db_organization_sic
import db_organization_naics as module_db_organization_naics
import db_organization_ticker as module_db_organization_ticker
import db_organization_employee as module_db_organization_employee

import pandas as pd

file_handler = logging.FileHandler(filename='equifax_validation.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]

# format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=handlers
)

logging.info('blue-dog Ingest Validation Utility')
logging.info('The application is starting')

test123 = "HELLO"
logging.warning('Test data: %s', test123)

global test_count
global pass_count
global error_count

test_count = 0
pass_count = 0
error_count = 0

logging.debug("Starting postgresql-connect")
logging.debug("Set input source")

# Input file options

# Selection - 17 records
# source_file = 'C://macgowan//projects//qa//ingest_test_20191009//data//equifax//full//domestic//West_Linkage_Domestic_full_file_2019_07//test_17.txt'

# Selection - 20000 records
# source_file = 'C://macgowan//projects//qa//ingest_test_20191009//data//equifax//full//domestic//West_Linkage_Domestic_full_file_2019_07//test_20000.txt'

# Selection - 30 files from full collection 0/20/50/75/100
source_file = 'C://macgowan//projects//qa//ingest_test_20191009//data//equifax//full//domestic//West_Linkage_Domestic_full_file_2019_07//test_select_30.txt'

# Full - 54,000,000 records - this will fail to load !
# source_file = 'C://macgowan//projects//qa//ingest_test_20191009//data//equifax//full//domestic//West_Linkage_Domestic_full_file_2019_07//West_Linkage_Domestic_full_file_2019_07.txt'


logging.debug("Source file: %s", source_file)
logging.debug("Read the input file using pandas")
pipe_data = pd.read_csv(source_file, sep='|')

# Here is the plan
# We will iterate the file able and we will then process a selected number
# of the rows for testing
# passing the row to the validation method

results = module_results.Results()

db_organization_core = module_db_organization_core.DbOrganizationCore("John", 36, pipe_data)
db_organization_name = module_db_organization_name.DbOrganizationName()
db_organization_address = module_db_organization_address.DbOrganizationAddress()
db_organization_phone = module_db_organization_phone.DbOrganizationPhone()
db_organization_email = module_db_organization_email.DbOrganizationEmail()
db_organization_website = module_db_organization_website.DbOrganizationWebsite()
db_organization_sic = module_db_organization_sic.DbOrganizationSic()
db_organization_naics = module_db_organization_naics.DbOrganizationNaics()
db_organization_ticker = module_db_organization_ticker.DbOrganizationTicker()
db_organization_employee = module_db_organization_employee.DbOrganizationEmployee()

# The interval is used to select a row each interneal. Ie if the interablt is 1000
# we will wait until a though rowna na bee aj drawn t he table

internal_count = 0
internal_set = 1

logging.debug("internal_count: %i", internal_count)
logging.debug("internal_set: %i", internal_set)

for index, row in pipe_data.iterrows():
    if index > internal_count:
        internal_count += internal_set
        logging.debug("Process for internal: %i", internal_count)
        # print(index, row['EFXID'], row['EFX_ADDRESS'])

        db_organization_core.validation(row, results)
        db_organization_name.validation(row, results)
        db_organization_address.validation(row, results)

        db_organization_phone.validation(row, results)
        db_organization_phone.validation_fax(row, results)

        db_organization_email.validation(row, results)
        db_organization_website.validation(row, results)

        db_organization_sic.validation_primary(row, results)
        db_organization_sic.validation_secondary(row, 2, results)
        db_organization_sic.validation_secondary(row, 3, results)
        db_organization_sic.validation_secondary(row, 4, results)
        db_organization_sic.validation_secondary(row, 5, results)

        db_organization_naics.validation_primary(row, results)
        db_organization_naics.validation_secondary(row, 2, results)
        db_organization_naics.validation_secondary(row, 3, results)
        db_organization_naics.validation_secondary(row, 4, results)
        db_organization_naics.validation_secondary(row, 5, results)

        db_organization_ticker.validation(row, results)

        db_organization_employee.validation(row, results)

# prints Time_(s), Mass_Flow_(kg/s), ...
logging.debug("Print columns: %s", pipe_data.columns)

# print the EFXID column
# print("Print data: ", pipe_data['EFX_ADDRESS'])

pipe_id_array = pipe_data['EFXID']
pipe_address_array = pipe_data['EFX_ADDRESS']

logging.debug("Data - id : %s", pipe_id_array[0])
logging.debug("Print - pipe_address_array: %s", pipe_address_array[0])

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
    postgreSQL_select_Query = "SELECT * FROM troa_dev.organization_address where partitionid = '52'"

    cursor.execute(postgreSQL_select_Query)
    logging.debug("Selecting rows from mobile table using cursor.fetchall")
    mobile_records = cursor.fetchall()

    logging.debug("Print each row and it's columns values")
    for row in mobile_records:
        logging.debug("partition = %s", row[0])
        logging.debug("partitionid = %s", row[1])
        logging.debug("addresstype  = %s", row[2])
        logging.debug("streetaddress1  = %s", row[3])

except Exception as err:
  logging.error("An exception occurred")
  # print Exception, err

  traceback.print_tb(err.__traceback__)

test1 = 123
test2 = 123

try:
   assert test1 == test2
   logging.debug("TEST SUCCESSFULE")
except:
    logging.debug("TEST FAILED")

# Test Validation class

logging.debug("Calling test class: module_db_organization_core.DbOrganizationCore()")
p1 = module_db_organization_core.DbOrganizationCore("John", 36, pipe_data)
p1.myfunc()


print(p1.name)
print(p1.age)

logging.info(' ')
logging.info("********************************************************************************")
logging.info("**** TEST RESULTS **************************************************************")
logging.info("********************************************************************************")
logging.info("Calling test class: results")
logging.info("Results - test_passed: %d", results.get_passed())
logging.info("Results - test_failed: %d", results.get_failed())
logging.info("Results - test_no_data: %d", results.get_no_data())
logging.info("Results - test_exception: %d", results.get_exception())
logging.info("Results - test_total: %d", results.get_total())
logging.info(' ')
logging.info(' ')

logging.info('The end if near!')
logging.info('blue-dog Ingest Validation Utility')
logging.info('The application is stopping')
logging.info('Have a nice day')
