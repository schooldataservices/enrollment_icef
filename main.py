from google.cloud import bigquery
from modules.bq import *
from modules.file_transformation import *
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/sam/icef-437920.json'
client = bigquery.Client(project='icef-437920')


returning = get_returning_students(client)
new = get_new_students(client)
df = assimilate_new_and_returning(new, returning) #need to add demographic info from PS in a merge. New students demographic info is not available through SM. 

#google sheets scrape needs to be iterated to include all other sheets, becasue that breaks down budgeted enrollment by grade. 
#The current budgeted enrollment got appeneded to intent to return. But might just need to be a seperate table.

#Also must normalize the grades in teh file_transofmration. Can not have "second" and '2nd' in the same column.