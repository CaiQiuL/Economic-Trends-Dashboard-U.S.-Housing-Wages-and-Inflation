import pandas as pd
from datetime import datetime

def process():
    df = pd.read_csv('Average Sale Price of Houses Sold - US Census Bureau.csv')
    #Extracts the CSV into a Pandas dataframe

    df['observation_date'] = pd.to_datetime(df['observation_date'])
    #Converts the 'observation_date' column into a datetime object
    
    df['Year'] = df['observation_date'].dt.year
    #Extract the year value from 'observation_date' into a 'year' column

    yearly_sum = df.groupby('Year', as_index=False)['ASPUS'].mean()
    #Create a new dataframe yearly_sum, where the first column is the year, and the 2nd is the mean of the ASPUS

    yearly_sum.to_csv('Cleaned Average Sale Price of Houses Sold.csv', index=False)
    #Convert the cleaned data into a CSV for usage



    yearlyWage = pd.read_csv('National Average Wage Index-SSA.txt', sep='\t')
    #Extracts the TSV into a Pandas dataframe

    yearlyWage.to_csv('Cleaned Avereage Wage Index.csv', index=False)
    #Convert the cleaned data into a CSV for usage




    df = pd.read_csv('U.S. Bureau of Labor Statistics-Annual Average price for eggs.txt', sep='\t')
    #Extracts the TSV into a Pandas dataframe

    yearPrice = df.iloc[:,:2]
    #Filters only by the first 2 rows, which are the only data points needed. 

    yearPrice.to_csv('Cleaned Egg Price.csv', index=False)
    #Convert the cleaned data into a CSV for usage

    double_merged_df = pd.merge(yearly_sum, yearlyWage, on='Year', how='outer')
    #Outer join the dataframes into a merged dataframe awaiting the final merge

    merged_df = pd.merge(double_merged_df, yearPrice, on='Year', how='outer')
    #Outer join the combined dataframes into a single dataframe and include NaN values

    
    merged_df.to_csv('Completedly Process Data.csv', index=False)
    
def processCPI():

    df = pd.read_csv('CPI.txt', sep='\t')

    filtered_df = df.iloc[:, [0,13,15]].copy()

    CurrentCPI = [317.671, 319.082,319.799, 320.795, 321.465, 322.561,323.048,323.976,324.800]

    AverageCurrentCPI = round(sum(CurrentCPI)/len(CurrentCPI), 3)


    filtered_df.loc[len(filtered_df)] = ['2025', AverageCurrentCPI, '--']

    filtered_df.to_csv('Cleaned Yearly CPI.csv', index=False)


def merge(file1, file2, name):
    df1=pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    merged_df = pd.merge(df1,df2, on='Year', how='outer')

    merged_df.to_csv(name, index=False)

merge('Cleaned Data v0 - Completedly Process Data.csv', 'Cleaned Yearly CPI.csv', 'Aggregate Clean Data v1.csv')

