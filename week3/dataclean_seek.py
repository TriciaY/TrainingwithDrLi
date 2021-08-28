import pandas as pd
import numpy as np
import re

columns = ['job_title', 'job_link', 'job_advertiser', 'location', 'released_time', 'classification']

# remove duplicated contents
def deduplication(x):
    temp = x.strip()
    index = (temp + temp).find(temp, 1)
    if index != -1:
        return temp[:index]
    else:
        return temp

#rename the columns
def modify_input(df):
    
    df=df[ ~ df[0].str.contains('字段')]
    
    if df.shape[1] == 7:
        df.drop(df.columns[4], inplace=True, axis=1)
    
    df.columns = columns
    
    return df

#deal with missing values in column 3: job_advertiser
def fill_advertiser(df):
    df.job_advertiser.fillna('Private Advertiser', inplace=True)
    return df

# clean column 4: 'location'
def clean_location(df):
    #split the column into two columns
    df[['location', 'area']] = df.location.str.split('area:', expand=True)

    #get rid of ', ' in "location"
    df.location = df.location.str.replace('location:', '')
    #get rid of info other than location
    df[['location', 'locationN']] = df.location.str.split(',', expand=True)
    df.drop('locationN', inplace=True, axis=1)

    # "area": some rows with salary info, delete these info
    df[['area', 'saTemp']] = df.area.str.split(',', n=1, expand=True)
    df.drop('saTemp', inplace=True, axis=1)

    # remove the duplicated contents in "location", "area"
    df.location = df.location.astype(str)
    df.area = df.area.astype(str)
    df.location = df.location.apply(deduplication)
    df.area = df.area.apply(deduplication)
    return df

# clean column 5: time_released
def clean_time(df):
    # extract time,if contains number, return the number and the next character,
    # otherwise return 0, i.e., treat "featrued" as 0
    def extract_time(x):
        if re.match(r'(\d+[a-z])\s', x):
            return re.match(r'(\d+[a-z])\s', x).group(1)
        else:
            return 0

    df.released_time = df.released_time.astype(str)
    df['released_days'] = df.released_time.apply(extract_time)

    # convert contents in 'time' into unified time with 'day' as a unified unit of measurement
    # 'm' is treated as 'minute'
    def convert_time(x):
        if 'd' not in x:
            return 0
        else:
            day = int(x[:-1])
            return day

    df.released_days = df.released_days.astype(str)
    df.released_days = df.released_days.apply(convert_time)

    df.drop('released_time', inplace=True, axis=1)

    return df

# clean "classification"

def clean_classification(df):
    # extract 'salary' from 'classification'
    def extract_salary(cl):
        if 'classification:' in cl:
            return 'unknown'
        else:
            return cl

    df['salary'] = df.classification.apply(extract_salary)

    # extract 'classification' from 'classification'
    def extract_classification(cl):
        if 'classification:' in cl:
            return cl
        else:
            return 'unknown'

    df.classification = df.classification.apply(extract_classification)
    df.classification = df.classification.str.replace('classification: ', '')
    df[['classification', 'subclassification']] = df.classification.str.split('subClassification:',
                                                                                          expand=True)
    df.classification = df.classification.astype(str)
    df.classification = df.classification.apply(deduplication)
    df.subclassification = df.subclassification.astype(str)
    df.subclassification = df.subclassification.apply(deduplication)

    return df

# clean "salary"
def clean_salary(df):
    # treat all rows without a number as 'unknown'
    def convert_nonN(x):
        num = bool(re.search(r'\d', x))
        if num:
            return x
        else:
            return None

    df.salary = df.salary.apply(convert_nonN)
    df.salary = df.salary.str.replace(',','')
    df.salary = df.salary.str.replace('to', '-', regex=True)
    df.salary = df.salary.str.replace(r'(\d+)\s(\d+)', r'\1\2', regex=True)

    df[['min_salary', 'max_salary']] = df.salary.str.split('-', n=1, expand=True)
    df.drop('salary', inplace=True, axis=1)

    df.min_salary = df.min_salary.str.replace(r'(\d+)k', r'\1000', case=False, regex=True)

    def extract_num(x):
        if re.match(r'[^0-9]*(\d+\.*\d*)[^0-9]*', x):
            return re.match(r'[^0-9]*(\d+\.*\d*)[^0-9]*', x).group(1)
        else:
            return 'unknown'

    df.min_salary = df.min_salary.astype(str)
    df.min_salary = df.min_salary.apply(extract_num)

    def convert_annual(x):
        if x == 'unknown':
            return 'unknown'
        else:
            x = float(x)
            if x <= 8:
                return 'unknown'
            elif 8 < x < 50:  # hourly
                return x * 8 * 260  # working 8 hours a day, and 260 days a year
            elif x == 60 or x == 70:
                return x * 1000
            else:
                return x

    df.min_salary = df.min_salary.apply(convert_annual)

    df.max_salary = df.max_salary.str.replace('k', '000', case=False)
    df.max_salary = df.max_salary.astype(str)
    df.max_salary = df.max_salary.apply(extract_num)
    df.max_salary = df.max_salary.apply(convert_annual)

    df.min_salary[
        (df['min_salary'] == 'unknown') & (df['max_salary'] != 'unknown')] = df.max_salary

    df.max_salary[
        (df['min_salary'] != 'unknown') & (df['max_salary'] == 'unknown')] = df.min_salary

    min_salaryValued = df['min_salary'].unique()[1:]
    minMax = min_salaryValued.max()
    minMin = min_salaryValued.min()

    max_salaryValued = df['max_salary'].unique()[1:]
    maxMax = max_salaryValued.max()
    maxMin = max_salaryValued.min()

    for r, row in enumerate(df['min_salary'].values):
        if row == 'unknown':
            df['min_salary'][r] = np.random.randint(minMin, minMax, size=1)[0]

    for r, row in enumerate(df['max_salary'].values):
        if row == 'unknown':
            df['max_salary'][r] = max(np.random.randint(maxMin, maxMax, size=1)[0], df['min_salary'][r])

    return df

def clean_job(file_path):
    df = pd.read_excel(file_path,header=None)
    df = modify_input(df)
    df = fill_advertiser(df)
    df = clean_location(df)
    df = clean_time(df)
    df = clean_classification(df)
    df = clean_salary(df)

    return df


df_admin = clean_job('NZ_Admin_JOBS.xlsx')
df_admin.to_excel('NZ_Admin_JOBS1.xlsx')

#df_banking = clean_job('NZ_Banking_JOBS.xlsx')
#df_banking.to_excel('NZ_Banking_JOBS1.xlsx')
