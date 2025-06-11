import pandas as pd
import numpy as np
import logging




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
    INNER JOIN `icef-437920.enrollment.intent_to_return_results` itr
    ON stt.student_number = itr.student_id;
    '''
    df = client.query(query).to_dataframe()


    logging.info(f'Student returning info:\n {(df['student_returning'].value_counts())}')

    df = df[
    (df['student_returning'] == 'Yes') |
    (
        (df['student_returning'] == 'Maybe') &
        (df['school_attending'].notna()) &
        (df['school_attending'] != 'TBD')
    )
]

    df['new_or_returning'] = 'returning'

    grade_to_int_mapping = {
    'TK': -1,
    'K': 1,
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

    df = create_new_school_column(df)

    return(df)

# If School is VPMS and grade 9 default to school_attending column. 
# If School is IILA, IVEA, VPES, or IIECA and grade 6 default to school_attending column.

# Otherwise populate the new column with the school column 
#Level 10 difficulty here 
def create_new_school_column(returning):

    #Chagne program id to be based on school_attending column only if it is present
    school_attending_dict = {
        'Vista Middle': 10396, 
        'View Park Middle': 10007, 
        'View Park High School': 10395 
    }

    # Update program_id conditionally
    returning['program_id'] = returning.apply(
        lambda row: school_attending_dict[row['school_attending']] 
        if row['school_attending'] in school_attending_dict 
        else row['program_id'],
        axis=1
    )

    #Change the school_attending column to be an acronym 
    school_attending_dict = {
    'Vista Middle': 'IVMA', #10396
    'View Park Middle': 'VPMS', #10007
    'View Park High School': 'VPHS' #10395
    }

    returning['school_attending'] = returning['school_attending'].map(school_attending_dict)

    # Create a new column 'final_school' where school_attending overrides school if not NaN
    returning['final_school'] = returning['school_attending'].combine_first(returning['school'])
    returning = returning.drop(columns=['school', 'school_attending', 'source', 'student_returning'])
    returning = returning.rename(columns={'final_school': 'school'})

    return(returning)



def get_new_students(client):
    query = '''
    SELECT * FROM `icef-437920.enrollment.completed_registrations`
    '''
    sm = client.query(query).to_dataframe()
    
    sm['student_name'] = sm['lname'] + ', ' + sm['fname']

    sm['new_or_returning'] = 'new'

    sm = sm[['student_id', 'student_name', 'school_name', 'grade', 'new_or_returning']]
    sm = sm.rename(columns={'school_name': 'school'})

    sm['grade'] = sm['grade'].replace({'TK': -1, 'K': 0})
    
    new_students_program_ids = {'ICEF Inglewood Elementary Charter Academy': 10392,
                            'ICEF Innovation Los Angeles Charter School': 10393,
                            'ICEF View Park Preparatory Elementary': 10394,
                            'ICEF Vista Elementary Charter Academy': 12239,
                            'ICEF View Park Preparatory High School': 10395,
                            'ICEF View Park Preparatory Middle': 10007,
                            'ICEF Vista Middle Charter Academy': 10396
                            }    

    sm['program_id'] = sm['school'].map(new_students_program_ids)
    
    program_school_mapping = {
    10396: "IVMA",
    10393: "IILA",
    10392: "IIECA",
    10394: "VPES",
    12239: "IVEA",
    10007: "VPMS",
    10395: "VPHS"
    }

    sm['school'] = sm['program_id'].map(program_school_mapping)


    return(sm)

# def mark_transitioning_students(df):
#     """
#     Marks students as transitioning to ICEF middle or high school based on school_code_registering.

#     Args:
#         df (pd.DataFrame): The input DataFrame containing the column 'school_code_registering'.

#     Returns:
#         pd.DataFrame: The updated DataFrame with a new column 'transitioning_to_icef_middle_or_hs'.
#     """
#     # Define the school codes for transitioning
#     transitioning_codes = [506, 953, 543]

#     # Assign 'Yes' or 'No' based on the condition
#     df['transitioning_to_icef_middle_or_hs'] = df['school_code_registering'].apply(
#         lambda x: 'Yes' if x in transitioning_codes else 'No'
#     )

#     return df


def create_incoming_students(new, returning):

    df = pd.concat([new, returning])
    df = df.reset_index(drop=True)

    return(df)



def add_total_rows_by_program(df):
    """
    Adds a 'total' row for each program_id in the DataFrame by summing the
    'new', 'returning', 'total_enrollment', and 'budgeted_enrollment' columns.

    The 'school' column in the total rows will contain the corresponding school acronym.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing at least the following columns:
        'program_id', 'school', 'grade', 'new', 'returning', 'total_enrollment', 'budgeted_enrollment'

    Returns:
    - pd.DataFrame: A new DataFrame with total rows appended.
    """

    # Group and sum numeric columns
    totals = (
        df.groupby("program_id")[["new", "returning", "total_enrollment", "budgeted_enrollment"]]
        .sum()
        .reset_index()
    )

    # Get first school acronym per program_id
    school_map = df.groupby("program_id")["school"].first().reset_index()

    # Merge to get the school acronym for totals
    totals = totals.merge(school_map, on="program_id", how="left")

    # Add the grade column as 'total'
    totals["grade"] = "total"

    # Reorder columns to match original df
    totals = totals[df.columns]

    # Append totals to original df
    combined = pd.concat([df, totals], ignore_index=True)

    # Sort so totals appear after each program_id's grades
    combined = combined.sort_values(
        by=["program_id", "grade"],
        key=lambda col: col.astype(str)
    ).reset_index(drop=True)

    return combined


#The budgeted_enrollment table columns are brought in from intent to return. This is generated in google sheets hookups. 
#Therefore those are the budgeted enrollment numbers that should be used
def create_budgeted_enrollment(df, client):

    df['grade'] = df['grade'].astype(str)

    #create unique students by school in order to get the total enrollment by school and grade. 
    unique_students_by_school_grade = df.groupby(['program_id', 'school', 'grade', 'new_or_returning'])['student_id'].nunique().reset_index()
    unique_students_by_school_grade = unique_students_by_school_grade.rename(columns={'student_id': 'total_enrollment'})

    unique_students_by_school_grade = unique_students_by_school_grade.pivot_table(
    index=["program_id", "school", "grade"],
    columns="new_or_returning",
    values="total_enrollment",
    fill_value=0  # Optional: fills NaN with 0
    ).reset_index()

    unique_students_by_school_grade.columns.name = None


    unique_students_by_school_grade['total_enrollment'] = unique_students_by_school_grade['new'] + unique_students_by_school_grade['returning'] 

    query = '''
        SELECT
            *
        FROM
        `icef-437920.enrollment.budgeted_enrollment`
        '''

    budgeted_enrollment = client.query(query).to_dataframe()
    budgeted_enrollment["grade"] = budgeted_enrollment["grade"].apply(lambda x: "total" if pd.isna(x) else str(int(x)))

    budgeted_enrollment = pd.merge( unique_students_by_school_grade, budgeted_enrollment, on=['program_id', 'grade'], how='left')
    budgeted_enrollment = budgeted_enrollment.drop(columns=['school_y'])
    budgeted_enrollment = budgeted_enrollment.rename(columns={'school_x': 'school'})


    # budgeted_enrollment = budgeted_enrollment.loc[~budgeted_enrollment['budgeted_enrollment'].isna()].reset_index(drop=True) 
    budgeted_enrollment['budgeted_enrollment'] = budgeted_enrollment['budgeted_enrollment'].fillna(0)

    budgeted_enrollment = add_total_rows_by_program(budgeted_enrollment)

    budgeted_enrollment['enrollment_capacity'] = (budgeted_enrollment['total_enrollment'] / 
                                            budgeted_enrollment['budgeted_enrollment']).round(2)
    
    budgeted_enrollment['seats_available'] =  budgeted_enrollment['budgeted_enrollment'] - budgeted_enrollment['total_enrollment']
    budgeted_enrollment['total_enrollment'] = budgeted_enrollment['total_enrollment'].astype(int)
    budgeted_enrollment['new'] = budgeted_enrollment['new'].astype(int)
    budgeted_enrollment['returning'] = budgeted_enrollment['returning'].astype(int)

    total_students = budgeted_enrollment.loc[budgeted_enrollment['grade'] == 'total']['new'].sum()
    logging.info(f'Here is the amount of total students: {total_students}')

    return budgeted_enrollment




    # return(budgeted_enrollment)


#CHECK
# temp = intent_to_return.loc[intent_to_return['School'] == 'View Park High School']
# temp.groupby(['Grade', 'Student Returning']).count()
