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
    SELECT * FROM `icef-437920.powerschool.intent_to_return_results`
    '''
    df = client.query(query).to_dataframe()

    df.loc[df['student_returning'] == 'Yes']

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
        (sm['r_withdrawn'] == 1) &
        (sm['r_sis_export_status'] == 'Succeeded'), :]
    
    sm['student_name'] = sm['lname'] + ', ' + sm['fname']

    sm['new_or_returning'] = 'new'
    sm['source'] = '' #can check new columns brought in from the query
    sm['transitioning_to_icef_middle_or_hs'] = '' #check source bright in from query to see if coming from ICEF elem


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


def assimilate_new_and_returning(new, returning):
    new['program_id'] = new['program_id'].fillna(0)
    new['program_id'] = new['program_id'].astype(int)
    program_id_school_dict = returning[['program_id', 'school']].drop_duplicates().set_index('program_id')['school'].to_dict()
    program_id_budget_dict = returning[['program_id', 'budgeted_enrollment']].drop_duplicates().set_index('program_id')['budgeted_enrollment'].to_dict()


    new['school'] = new['program_id'].map(program_id_school_dict)
    new['budgeted_enrollment'] = new['program_id'].map(program_id_budget_dict)
    new = mark_transitioning_students(new)


    new = new[['student_id', 'student_name', 'school', 'grade', 'source', 'transitioning_to_icef_middle_or_hs', 'program_id', 'budgeted_enrollment', 'new_or_returning']]

    df = pd.concat([new, returning])
    df = df.reset_index(drop=True)

    return(df)


