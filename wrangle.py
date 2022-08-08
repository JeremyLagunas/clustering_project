import pandas as pd
import numpy as np
import os
from env import host, user, password
import sklearn.preprocessing


# Acquire

def get_connection(db, user = user, host = host, password = password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
# This function will be used in other functions to connect with the required databases. 

def new_zillow_data():
	# this query pulls houses that had a transaction date in 2017 and are considered to be single family residential properties
    
    sql_query = """
                   SELECT prop.*,
        predictions_2017.logerror,
        predictions_2017.transactiondate,
        air.airconditioningdesc,
        arch.architecturalstyledesc,
        build.buildingclassdesc,
        heat.heatingorsystemdesc,
        land.propertylandusedesc,
        story.storydesc,
        type.typeconstructiondesc
        FROM properties_2017 prop
        JOIN (
            SELECT parcelid, MAX(transactiondate) AS max_transactiondate
            FROM predictions_2017
            GROUP BY parcelid
            ) pred USING(parcelid)
        JOIN predictions_2017 ON pred.parcelid = predictions_2017.parcelid
                          AND pred.max_transactiondate = predictions_2017.transactiondate
        LEFT JOIN airconditioningtype air USING(airconditioningtypeid)
        LEFT JOIN architecturalstyletype arch USING(architecturalstyletypeid)
        LEFT JOIN buildingclasstype build USING(buildingclasstypeid)
        LEFT JOIN heatingorsystemtype heat USING(heatingorsystemtypeid)
        LEFT JOIN propertylandusetype land USING(propertylandusetypeid)
        LEFT JOIN storytype story USING(storytypeid)
        LEFT JOIN typeconstructiontype type USING(typeconstructiontypeid)
        WHERE propertylandusedesc = "Single Family Residential"
            AND transactiondate <= '2017-12-31'
            AND prop.longitude IS NOT NULL
            AND prop.latitude IS NOT NULL
                   ;
                   """
    df = pd.read_sql(sql_query, get_connection('zillow'))
    return df

# Prepare

def null_counter(df):
    # This function will show the number of rows missing in a column and the percent of rows missing per column. 
    # Make new columns names:
    new_columns = ['name', 'num_rows_missing', 'pct_rows_missing']
    # Turn into a DF
    new_df = pd.DataFrame(columns = new_columns)
    # Loop to determine number of missing columns and pertentage of missing columns:
    for col in list(df.columns):
        num_missing = df[col].isna().sum()
        pct_missing = num_missing / df.shape[0]
        
        add_df = pd.DataFrame([{'name':col, 'num_rows_missing':num_missing, 'pct_rows_missing':pct_missing}])
        
        new_df = pd.concat([new_df, add_df], axis = 0)
        
    new_df.set_index('name', inplace=True)
    
    return new_df

# Function to ensure all properties are single unit properties
def zillow_units(df):
    df = df.drop(df[df.unitcnt == 2.0].index)
    df = df.drop(df[df.unitcnt == 3.0].index)
    return df

# Function that drops nulls based on a specified proportion those nulls make up of the data
def null_dropper(df, prop_required_col, prop_required_row):
    
    prop_null_col = 1 - prop_required_col
    # Loop to count nulls and calculate the percentage of nulls
    for col in list(df.columns):
        
        null_sum = df[col].isna().sum()
        null_pct = null_sum / df.shape[0]
      # If statement which determines whether the column contains enough nulls to drop.  
        if null_pct > prop_null_col:
            df.drop(columns=col, inplace=True)
            
    row_threshold = int(prop_required_row * df.shape[1])
        
    df.dropna(axis = 0, thresh=row_threshold, inplace=True)
    
    return df

# Over arching clean function which takes all the smaller functions and adds them together - in progress
def zillow_clean(df):
    # Remove small amount of remaining nulls.
    df = df.dropna()
    # Drop comuns with redundant information. 
    df = df.drop(columns=['finishedsquarefeet12', 'roomcnt', 'censustractandblock', 'landtaxvaluedollarcnt', 'taxamount', 'structuretaxvaluedollarcnt', 'propertycountylandusecode'])
    df= df.drop(columns=['propertylandusetypeid'])
    df = df.drop(columns=['propertylandusedesc'])
    # Create a new column showing the number of half baths
    df['half_bath'] = (df.bathroomcnt - df.fullbathcnt)/0.5
    # Renaming columns to make them easier to work with
    df.rename(columns={'bedroomcnt':'bed', 'bathroomcnt':'bath', 'calculatedfinishedsquarefeet':'square_feet', 'fips':'county', 'lotsizesquarefeet':'lot_square_feet', 'regionidcity':'id_city', 'regionidcounty':'id_county', 'regionidzip':'id_zip', 'taxvaluedollarcnt':'appraisal'}, inplace = True)
    # Changing the fips column to a county column
    df['county'].replace({6037.0: 'LA County', 6059.0: 'Orange County',
               6111.0: 'Ventura County'}, inplace=True)
    
    return df