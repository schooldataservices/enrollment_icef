import pandas as pd

def seperate_apps_registrations(sm):

    # Separate columns with 'r_' prefix into a new DataFrame
    r_columns = [col for col in sm.columns if col.startswith('r_')]
    reg = sm[r_columns]

    # Remove the 'r_' prefix from the column names in the new DataFrame
    reg.columns = [col[2:] for col in r_columns]

    # Drop the 'r_' columns from the original DataFrame
    apps = sm.drop(columns=r_columns)

    return(reg, apps)



def get_returning_students(client):
    query = '''
    SELECT itr.*
    FROM `icef-437920.views.student_to_teacher` stt
    INNER JOIN `icef-437920.powerschool.intent_to_return_results` itr
    ON stt.student_number = itr.student_id;
    '''
    df = client.query(query).to_dataframe()

    df = df.loc[(df['student_returning'] == 'Yes') | (df['student_returning'] == 'Maybe')]

    df['new_or_returning'] = 'returning'
    df.drop(columns=['student_returning'], inplace=True)

    return(df)


def get_new_students(client):
    query = '''
    SELECT * FROM `icef-437920.powerschool.application_registration_export_2025`
    '''
    sm = client.query(query).to_dataframe()

 
    sm = sm[['fname', 'lname', 'student_id', 'id', 'student_annual_id', 'r_school_id', 'r_application_id', 'r_updated_at', 'r_withdrawn', 'r_status', 'r_type', 'r_sis_export_status', 'grade', 'program_id', 'school_name_previous', 'school_code_previous', 'school_name_registering', 'school_code_registering']]

    sm = sm.loc[(sm['r_type'] == 'enroll') & 
        (sm['r_withdrawn'] == 0) &
        (sm['r_sis_export_status'] == 'Succeeded'), :]
    
    sm['student_name'] = sm['lname'] + ', ' + sm['fname']

    sm['new_or_returning'] = 'new'
    sm['source'] = '' #can check new columns brought in from the query
    sm['transitioning_to_icef_middle_or_hs'] = '' #check source bright in from query to see if coming from ICEF elem

    #Chagne grade values to match what is coming from Google Sheets. 
    grade_mapping = {
    'TK': 'TK',
    'K': 'Kindergarten',
    '1': '1st Grade',
    '2': '2nd Grade',
    '3': '3rd Grade',
    '4': '4th Grade',
    '5': '5th Grade',
    '6': '6th Grade',
    '7': '7th Grade',
    '8': '8th Grade',
    '9': '9th Grade',
    '10': '10th Grade',
    '11': '11th Grade',
    '12': '12th Grade'
     }

    # Replace the values in the 'grade' column using the mapping
    sm['grade'] = sm['grade'].replace(grade_mapping)

    sm['student_id'] = None
    

    return(sm)

def mark_transitioning_students(df):
    """
    Marks students as transitioning to ICEF middle or high school based on school_code_registering.

    Args:
        df (pd.DataFrame): The input DataFrame containing the column 'school_code_registering'.

    Returns:
        pd.DataFrame: The updated DataFrame with a new column 'transitioning_to_icef_middle_or_hs'.
    """
    # Define the school codes for transitioning
    transitioning_codes = [506, 953, 543]

    # Assign 'Yes' or 'No' based on the condition
    df['transitioning_to_icef_middle_or_hs'] = df['school_code_registering'].apply(
        lambda x: 'Yes' if x in transitioning_codes else 'No'
    )

    return df


def create_incoming_students(new, returning):
    new['program_id'] = new['program_id'].fillna(0)
    new['program_id'] = new['program_id'].astype(int)
    program_id_school_dict = returning[['program_id', 'school']].drop_duplicates().set_index('program_id')['school'].to_dict()
    # program_id_budget_dict = returning[['program_id', 'budgeted_enrollment']].drop_duplicates().set_index('program_id')['budgeted_enrollment'].to_dict()


    new['school'] = new['program_id'].map(program_id_school_dict)
    # new['budgeted_enrollment'] = new['program_id'].map(program_id_budget_dict)
    new = mark_transitioning_students(new)


    new = new[['student_id', 'student_name', 'school', 'grade', 'source', 'transitioning_to_icef_middle_or_hs', 'program_id', 'new_or_returning']]

   
    df = pd.concat([new, returning])
    df = df.reset_index(drop=True)

    df = df.drop(columns=['budgeted_enrollment', 'source']).reset_index(drop=True)


    grade_to_int_mapping = {
    'TK': -1,
    'Kindergarten': 0,
    '1st Grade': 1,
    '2nd Grade': 2,
    '3rd Grade': 3,
    '4th Grade': 4,
    '5th Grade': 5,
    '6th Grade': 6,
    '7th Grade': 7,
    '8th Grade': 8,
    '9th Grade': 9,
    '10th Grade': 10,
    '11th Grade': 11,
    '12th Grade': 12
    }

    df['grade'] = df['grade'].map(grade_to_int_mapping)
    df['grade']  = df['grade'] + 1

    #Drop grade 13
    df = df.loc[df['grade'] < 13].reset_index(drop=True)

    program_school_mapping = {
    10396: "IVMA",
    10393: "IILA",
    10392: "IIECA",
    10394: "VPES",
    12239: "IVEA",
    10007: "VPMS",
    10395: "VPHS"
    }

    df['school'] = df['program_id'].map(program_school_mapping)

    return(df)

def create_budgeted_enrollment_by_grade(client):

        query = '''
            SELECT
                program_id,
                grade,
                COUNT(*) AS budget_enrollment_by_grade
            FROM
                `icef-437920.powerschool.intent_to_return_results`
            GROUP BY
                program_id,
                grade
            ORDER BY
                program_id,
                grade
            '''

        budgeted_enrollment = client.query(query).to_dataframe()

        grade_to_int_mapping = {
        'TK': -1,
        'Kindergarten': 0,
        '1st Grade': 1,
        '2nd Grade': 2,
        '3rd Grade': 3,
        '4th Grade': 4,
        '5th Grade': 5,
        '6th Grade': 6,
        '7th Grade': 7,
        '8th Grade': 8,
        '9th Grade': 9,
        '10th Grade': 10,
        '11th Grade': 11,
        '12th Grade': 12
        }

        budgeted_enrollment['grade'] = budgeted_enrollment['grade'].map(grade_to_int_mapping)

        return(budgeted_enrollment)


#The budgeted_enrollment table columns are brought in from intent to return. This is generated in google sheets hookups. 
#Therefore those are the budgeted enrollment numbers that should be used
def create_budgeted_enrollment(df, client):
    unique_students_by_school_grade = df.groupby(['program_id', 'school', 'grade'])['student_id'].nunique().reset_index()
    unique_students_by_school_grade = unique_students_by_school_grade.rename(columns={'student_id': 'total_enrollment'})
  
    budgeted_enrollment_by_grade = create_budgeted_enrollment_by_grade(client) #Coming from intent to return

    budgeted_enrollment = pd.merge(unique_students_by_school_grade, budgeted_enrollment_by_grade, how='left', on=['grade', 'program_id'])

    #Drop rows where budgeted_enrollment is NaN. For grades where students can not return. i.e. 12 for High School
    budgeted_enrollment = budgeted_enrollment.loc[~budgeted_enrollment['budget_enrollment_by_grade'].isna()].reset_index(drop=True) 

    budgeted_enrollment['enrollment_capacity'] = (budgeted_enrollment['total_enrollment'] / 
                                            budgeted_enrollment['budget_enrollment_by_grade']).round(2)

    budgeted_enrollment['seats_available'] =  budgeted_enrollment['budget_enrollment_by_grade'] - budgeted_enrollment['total_enrollment']


    return(budgeted_enrollment)


#CHECK
# temp = intent_to_return.loc[intent_to_return['School'] == 'View Park High School']
# temp.groupby(['Grade', 'Student Returning']).count()
