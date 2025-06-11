from google.cloud import bigquery
from modules.bq import *
from modules.file_transformation import *
import os
import sys

logging.basicConfig(
    level=logging.INFO,  # Adjust as needed (e.g., DEBUG, WARNING)
    format="%(asctime)s - %(message)s",  # Log format
    datefmt="%d-%b-%y %H:%M:%S",  # Date format
    handlers=[
        logging.StreamHandler(sys.stdout)  # Direct logs to stdout
    ],
    force=True  # Ensures existing handlers are replaced
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/app/icef-437920.json'
client = bigquery.Client(project='icef-437920')


returning = get_returning_students(client) #Comes from Google Sheets
new = get_new_students(client) #Comes from School Mint
incoming = create_incoming_students(new, returning) #need to add demographic info from PS in a merge. New students demographic info is not available through SM. 
budgeted_enrollment = create_budgeted_enrollment(incoming, client)

send_to_gcs('enrollmentbucket-icefschools-1', save_path='', frame=incoming, frame_name='incoming_students.csv')
send_to_gcs('enrollmentbucket-icefschools-1', save_path='', frame=budgeted_enrollment, frame_name='budgeted_enrollment_capacity.csv')
