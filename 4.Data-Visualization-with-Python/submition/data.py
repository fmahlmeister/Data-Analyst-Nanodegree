# Import packages
import pandas as pd
import numpy as np
import os
import time
import requests
import bz2
import sqlite3

####################################################################################
####################################################################################
########################### Packages to get flights data ###########################
####################################################################################
####################################################################################

def get_url(start_year=1987, last_year=2008):

    """
    Create url list and filepath list

    Parameters
    ----------
    start_year : int (optional)
        the first year to start the download range
    last_year : int (optional)
        the last year to end the download range

    Returns
    ----------
    url : list of str
        List with complete url from the start_year to last_year
    filepath : list of str
        List with the complete filepath where files will be downloaded
    """
    
    # Create lists
    url, filepath = [], []
    
    for year in range(start_year,last_year+1):
    
        # Create full url string
        url_str = 'http://stat-computing.org/dataexpo/2009/'+str(year)+'.csv.bz2'

        # Append url string to the list
        url.append(url_str)

        # Create full filepath string
        filepath_str = 'source/'+str(year)+'.csv.bz2'

        # Append filepath string to the list
        filepath.append(filepath_str)

    return url, filepath



def get_download_and_unzip(filepath, url, force_download=False):

    """
    1. Download all the files from url list (bz2 format)
    2. Unzip bz2 format files to csv format
    3. Delete bz2 format files

    Parameters
    ----------
    url : list of str
        List with complete url from the start_year to last_year
    filepath : list of str
        List with the complete filepath where files will be downloaded
    force_download : bool (opitional)
        if True, force redownload of data

    Returns
    ----------
    d_start_l : list of time float
        List with download start time of each bz2 file
    d_end_l : list of time float
        List with download end time of each bz2 file
    u_start_l : list of time float
        List with unzip start time of each bz2 file
    u_end_l : list of time float
        List with unzip end time of each bz2 file
    """
    
    # Dictionary
    # download_start_time_list = d_start_l
    # download_end_time_list = d_end_l
    # unzip_start_time_list = u_start_l
    # unzip_end_time_list = u_end_l
    
    d_start_l, d_end_l = [], []
    u_start_l, u_end_l = [], []
    
    if force_download or not os.path.exists(filepath[:-4]):
        
        # -------- Calculate download time: start
        start_1 = time.time()
        
        urlData = requests.get(url)

        with open(filepath, mode ='wb') as file:
            file.write(urlData.content)
            
        # -------- Calculate download time: end
        end_1 = time.time()
        
        # -------- Calculate unzip time: start
        start_2 = time.time()
        
        # Open the file
        zipfile = bz2.BZ2File(filepath)
        
        # Get the decompressed data
        data = zipfile.read()
        
        # Remove the suffix (.bz2) at end of the file path
        newfilepath = filepath[:-4]
        
        # Write a uncompressed file at source folder
        open(newfilepath, 'wb').write(data)
        
        # -------- Calculate unzip time: end
        end_2 = time.time()
           
        # Delete zip files from source folder
        os.remove(filepath)
        
        # Add execution time to list
        d_start_l.append(start_1)
        d_end_l.append(end_1)
        u_start_l.append(start_2)
        u_end_l.append(end_2)
    else:
        print("All the files have already been downloaded")
    return d_start_l, d_end_l, u_start_l, u_end_l


def get_flights_data(url, filepath):

    """
    1. Create source directory
    2. Execute get_download_and_unzip function to all urls
    3. Calculate and shows execution time of each step (download & unzip)
    4. Calculate and shows total execution time (download & unzip) and the use of storage of the downloaded files

    Parameters
    ----------
    url : list of str
        List with complete url from the start_year to last_year
    filepath : list of str
        List with the complete filepath where files will be downloaded

    Returns
    ----------

    """
   
    total_d_start_l, total_d_end_l = [], []
    total_u_start_l, total_u_end_l = [], []
    total_d_diff_l, total_u_diff_l = [], []

    if not os.path.exists('source/'):
        os.mkdir('source/')
    else:
        pass

    for file in range(0,len(url)):

        d_start_l, d_end_l, u_start_l, u_end_l = get_download_and_unzip(filepath[file], url[file])

        total_d_start_l.append(d_start_l)
        total_d_end_l.append(d_end_l)
        total_u_start_l.append(u_start_l)
        total_u_end_l.append(u_end_l)

        total_d_diff = total_d_end_l[file][0]-total_d_start_l[file][0]
        total_d_diff_l.append(total_d_diff)

        total_u_diff = total_u_end_l[file][0]-total_u_start_l[file][0]
        total_u_diff_l.append(total_u_diff)

        total_diff = total_d_diff + total_u_diff

        print('')
        print(filepath[file][7:-4],':','file',file+1,'of',len(url),'was successfully downloaded and unzipped in',
              '{:0.2f}'.format(total_diff),'seconds')

    total_download_time = sum(total_d_diff_l)
    total_unzip_time = sum(total_u_diff_l)
    total_download_unzip = total_download_time + total_unzip_time

    print('-----------------------------------')
    print('total download time:','{:0.2f}'.format(total_download_time),'seconds')
    print('total unzip time:','{:0.2f}'.format(total_unzip_time),'seconds')
    print('')
    print('total execution time: ','{:0.2f}'.format((total_download_unzip)/60), 'minutes')

    statinfo = []

    for file in range(0,len(filepath)):

        file_bytes = os.stat(filepath[file][:-4]).st_size

        file_gb = file_bytes/1024**3

        statinfo.append(file_gb)

    print('total size of downloaded files:','{:0.2f}'.format(sum(statinfo)),'GB')


def get_supplemental_data():

    """
    Download supplemental files from http://stat-computing.org

    Parameters
    ----------

    Returns
    ----------

    """
    sup_files = ['airports','carriers','plane-data']

    for file in sup_files:
        filepath = 'source/'+file+'.csv'
        url = 'http://stat-computing.org/dataexpo/2009/'+file+'.csv'
        urlData = requests.get(url)

        with open(filepath, mode ='wb') as file:
            file.write(urlData.content)

####################################################################################
####################################################################################
################### Packages to create SQL tables with csv files ###################
####################################################################################
####################################################################################

def create_raw_table(conn):

    """
    Create empty sqlite table arranged to be filled with raw csv files

    Parameters
    ----------
    conn : str
        Connection object that represents the database

    Returns
    ----------

    """

    start = time.time()

    c = conn.cursor()

    c.execute('DROP TABLE IF EXISTS raw_data')
    
    sql_query = """CREATE TABLE raw_data (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                          Year INTEGER,
                                          Month INTEGER, 
                                          DayofMonth INTEGER,
                                          DayOfWeek INTEGER, 
                                          DepTime INTEGER,
                                          CRSDepTime INTEGER, 
                                          ArrTime INTEGER,
                                          CRSArrTime INTEGER, 
                                          UniqueCarrier TEXT,
                                          FlightNum INTEGER, 
                                          TailNum TEXT,
                                          ActualElapsedTime INTEGER, 
                                          CRSElapsedTime INTEGER,
                                          AirTime INTEGER, 
                                          ArrDelay INTEGER,
                                          DepDelay INTEGER, 
                                          Origin TEXT,
                                          Dest TEXT, 
                                          Distance INTEGER, 
                                          TaxiIn INTEGER,
                                          TaxiOut INTEGER, 
                                          Cancelled INTEGER,
                                          CancellationCode TEXT, 
                                          Diverted INTEGER,
                                          CarrierDelay INTEGER, 
                                          WeatherDelay INTEGER,
                                          NASDelay INTEGER, 
                                          SecurityDelay INTEGER,
                                          LateAircraftDelay INTEGER)"""
    c.execute(sql_query)

    end = time.time()
    print('total time:','{:0.0f}'.format((end-start)/60),'minutes')    
    
    c.close()
    conn.close()
    
    return print('Table created successfully')


def raw_data_entry(conn,start_year=1987,last_year=1988,chunksize=3000000,encoding='latin-1'):

    """
    Entry raw data from csv files to raw_data table

    Parameters
    ----------
    start_year : int (optimal)
        First csv file year to be insert into raw_data table

    last_year : int (optimal)
        Last csv file year to be insert into raw_data table

    chunksize : int (optimal)
        Chunksize of read_csv function

    encoding : str (optimal)
        Encoding of csv files 

    Returns
    ----------

    """

    start_1 = time.time()

    c = conn.cursor()

    for years in range(0,last_year-start_year+1):
        
        start_2 = time.time()

        for chunk in pd.read_csv('source/{}.csv'.format(start_year+years), 
                                 chunksize=chunksize,
                                 encoding=encoding):
            
            float_columns = []
            float_columns = (chunk.select_dtypes(['float'])).columns
            
            int_columns = []
            int_columns = (chunk.select_dtypes(['int'])).columns

            chunk.loc[:,float_columns] = chunk.loc[:,float_columns].fillna(0).astype(int)
            chunk.loc[:,int_columns] = chunk.loc[:,int_columns].fillna(0)
            
            chunk.to_sql(name="raw_data", con=conn, if_exists="append", index=False)
            print(chunk.iloc[0, 0])
            
            del chunk

        end_2 = time.time()
        print('time to include {}.csv:'.format(start_year+years),
              '{:0.0f}'.format(end_2-start_2),'seconds')

    end_1 = time.time()
    print('total time:','{:0.0f}'.format((end_1-start_1)/60),'minutes')
    
    c.close()
    conn.close()
    
    return print('Values inserted successfully')



def create_supl_tables(conn):

    """
    Create empty sqlite table of supplemental files, arranged to be filled with raw csv files:
    - Airports data file;
    - Carriers data file;
    - Plane-data file;

    Parameters
    ----------
    conn : str
        Connection object that represents the database

    Returns
    ----------

    """

    c = conn.cursor()
    
    # airports table
    c.execute('DROP TABLE IF EXISTS airports')
    
    sql_query = """CREATE TABLE airports (Id_airports INTEGER PRIMARY KEY AUTOINCREMENT,
                                          iata TEXT,
                                          airport TEXT,
                                          city TEXT,
                                          state TEXT,
                                          country TEXT,
                                          lat NUMERIC,
                                          long NUMERIC)"""
    c.execute(sql_query)
    
    print('airport table created successfully')
    
    # carriers table    
    c.execute('DROP TABLE IF EXISTS carriers')
    
    sql_query = """CREATE TABLE carriers (Id_carriers INTEGER PRIMARY KEY AUTOINCREMENT,
                                          Code TEXT,
                                          Description TEXT)"""
    c.execute(sql_query)
    
    print('carriers table created successfully')
   
    # plane_data table
    c.execute('DROP TABLE IF EXISTS plane_data')
    
    sql_query = """CREATE TABLE plane_data (Id_plane_data INTEGER PRIMARY KEY AUTOINCREMENT,
                                            tailnum TEXT,
                                            type TEXT,
                                            manufacturer TEXT,
                                            issue_date TEXT,
                                            model TEXT,
                                            status TEXT,
                                            aircraft_type TEXT,
                                            engine_type TEXT,
                                            year INTEGER)"""
    c.execute(sql_query)
    
    print('plane_data table created successfully')
    
    c.close()
    conn.close()

def supl_tables_data_entry(conn,encoding='latin-1'):

    """
    Entry raw supplemental data from csv files to airports, carriers and plane_data tables

    Parameters
    ----------
    conn : str
        Connection object that represents the database

    Returns
    ----------

    """

    c = conn.cursor()

    # airports table
    df = pd.read_csv('source/airports.csv',encoding=encoding)
    df.to_sql(name="airports", con=conn, if_exists="append", index=False)
    print('airports values inserted successfully')

    # carriers table
    df = pd.read_csv('source/carriers.csv',encoding=encoding)
    df.to_sql(name="carriers", con=conn, if_exists="append", index=False)
    print('carriers values inserted successfully')

    # plane_data table
    df = pd.read_csv('source/plane-data.csv',encoding=encoding)

    df.year = df.year.fillna(1900)
    df.year = df.year.replace(to_replace='None', value=1900)
    df.year = df.year.replace(to_replace=np.nan, value=1900)

    # float_columns = (df.select_dtypes(['float'])).columns
    # int_columns = (df.select_dtypes(['int'])).columns
    # obj_columns = (df.select_dtypes(['object'])).columns

    # df.loc[:,float_columns] = df.loc[:,float_columns].fillna(0)
    # df.loc[:,int_columns] = df.loc[:,int_columns].fillna(0)
    # df.loc[:,obj_columns] = df.loc[:,obj_columns].fillna(0)

    df.to_sql(name="plane_data", con=conn, if_exists="append", index=False)
    print('plane_data values inserted successfully')

    c.close()
    conn.close()

def create_data_table(conn):

    """
    Create data table from raw_data table and create Date column from Year, Month and DayofMonth atributes 

    Parameters
    ----------
    conn : str
        Connection object that represents the database

    Returns
    ----------

    """

    # Create table with selected data from raw_data
    start = time.time()

    c = conn.cursor()

    sql_query = """CREATE TABLE IF NOT EXISTS data AS
                                       SELECT Id,
                                              Year, 
                                              Month, 
                                              DayofMonth, 
                                              FlightNum, 
                                              Distance, 
                                              UniqueCarrier, 
                                              TailNum, 
                                              Origin, 
                                              Dest
                                         FROM raw_data;"""

    c.execute(sql_query)

    # Create Date column
    sql_query = """ALTER TABLE data 
                    ADD COLUMN Date datetime;"""

    c.execute(sql_query)

    # Fill Date column with Year, Month and DayofMonth atributes
    sql_query = """UPDATE data 
                      SET Date = Year || '-' || Month || '-' || DayofMonth"""

    c.execute(sql_query)

    # Create Index
    sql_query = """CREATE INDEX Date
                             ON data(Date);"""

    c.execute(sql_query)

    conn.commit()

    c.close()
    conn.close()

    end = time.time()
    print('total time:','{:0.0f}'.format((end-start)/60),'minutes')

####################################################################################
####################################################################################
##################### Packages to get SQL queries to DataFrame #####################
####################################################################################
####################################################################################


def chunk_preprocessing_numpy(chunk):

    """
    Optimize DataFrame columns which could be transformed into numpy arrays

    Parameters
    ----------
    chunk : pandas.DataFrame
        Chunk DataFrame

    Returns
    ----------
    chunk : pandas.DataFrame
        Chunk DataFrame with optimized column types

    """

    # data table
    try:
        chunk.loc[:,'Id'] = chunk.loc[:,'Id'].values.astype(np.int64)
    except:
        pass
    try:
        chunk.loc[:,'Date'] = pd.to_datetime(chunk.loc[:,'Date'].values, format='%Y-%m-%d')
    except:
        pass
    try:
        chunk.loc[:,'FlightNum'] = chunk.loc[:,'FlightNum'].values.astype(np.int16)
    except:
        pass
    try:
        chunk.loc[:,'Distance'] = chunk.loc[:,'Distance'].values.astype(np.int16)
    except:
        pass

    # airports table
    try:
        chunk.loc[:,'Id_airports'] = chunk.loc[:,'Id_airports'].values.astype(np.int64)
    except:
        pass
    # carriers table
    try:
        chunk.loc[:,'Id_carriers'] = chunk.loc[:,'Id_carriers'].values.astype(np.int64)
    except:
        pass       
    # plane_data table
    try:
        chunk.loc[:,'Id_plane_data'] = chunk.loc[:,'Id_plane_data'].values.astype(np.int64)
    except:
        pass   
    try:
        chunk.loc[:,'issue_date'] = pd.to_datetime(chunk.loc[:,'issue_date'].values, format='%m/%d/%Y')
    except:
        pass
    try:
        chunk.loc[:,'year'] = chunk.loc[:,'year'].values.astype(np.int32)
    except:
        pass
    return chunk
   
def df_processing_cat(df):  

    """
    Optimize Categorical columns of the DataFrame

    Parameters
    ----------
    df : pandas.DataFrame
        Chunk DataFrame

    Returns
    ----------
    df : pandas.DataFrame
        df DataFrame with column optimized column types

    """

    # data table
    try:
        df.loc[:,'UniqueCarrier'] = df.loc[:,'UniqueCarrier'].astype('category')
    except:
        pass
    try:
        df.loc[:,'TailNum'] = df.loc[:,'TailNum'].astype('category')
    except:
        pass
    try:
        df.loc[:,'Origin'] = df.loc[:,'Origin'].astype('category')
    except:
        pass
    try:
        df.loc[:,'Dest'] = df.loc[:,'Dest'].astype('category')
    except:
        pass

    # airports table
    try:
        df.loc[:,'iata'] = df.loc[:,'iata'].astype('category')
    except:
        pass
    try:
        df.loc[:,'airport'] = df.loc[:,'airport'].astype('category')
    except:
        pass
    try:
        df.loc[:,'airport1'] = df.loc[:,'airport1'].astype('category')
    except:
        pass
    try:
        df.loc[:,'airport2'] = df.loc[:,'airport2'].astype('category')
    except:
        pass
    try:
        df.loc[:,'city'] = df.loc[:,'city'].astype('category')
    except:
        pass
    try:
        df.loc[:,'state'] = df.loc[:,'state'].astype('category')
    except:
        pass
    try:
        df.loc[:,'country'] = df.loc[:,'country'].astype('category')
    except:
        pass

    # carriers table
    try:
        df.loc[:,'Code'] = df.loc[:,'Code'].astype('category')
    except:
        pass
    try:
        df.loc[:,'Description'] = df.loc[:,'Description'].astype('category')
    except:
        pass

    # plane_data table
    try:
        df.loc[:,'tailnum'] = df.loc[:,'tailnum'].astype('category')
    except:
        pass
    try:
        df.loc[:,'type'] = df.loc[:,'type'].astype('category')
    except:
        pass
    try:
        df.loc[:,'manufacturer'] = df.loc[:,'manufacturer'].astype('category')
    except:
        pass
    try:
        df.loc[:,'model'] = df.loc[:,'model'].astype('category')
    except:
        pass
    try:
        df.loc[:,'status'] = df.loc[:,'status'].astype('category')
    except:
        pass
    try:
        df.loc[:,'aircraft_type'] = df.loc[:,'aircraft_type'].astype('category')
    except:
        pass
    try:
        df.loc[:,'engine_type'] = df.loc[:,'engine_type'].astype('category')
    except:
        pass
    return df


def df_processing_cat_opt(df):  

    """
    Optimize Categorical columns of the DataFrame

    Parameters
    ----------
    df : pandas.DataFrame
        Chunk DataFrame

    Returns
    ----------
    df : pandas.DataFrame
        df DataFrame with column optimized column types

    """

    try:
        if col_obj.empty:
            col_obj = df.select_dtypes(include=['object']).columns
        else:
            df.loc[:,col_obj] = df.loc[:,col_obj].astype('category')
    except:
        pass

    return df


def query_to_df(query, conn = sqlite3.connect("source/all_data.db") , chunksize=500000):

    """
    Get SQL queries into DataFrames

    Parameters
    ----------
    query : str
        SQL query

    chunksize : int (optimal)
        Chunksize of read_sql_query function

    Returns
    ----------
    df : pandas.DataFrame
        df DataFrame with column optimized column types

    """

    start = time.time()
    
    c = conn.cursor()
    
    df = pd.DataFrame()
    chunk = pd.DataFrame()

    for chunk in pd.read_sql_query(sql=query, con=conn, chunksize=chunksize):

        df = pd.concat([df, chunk_preprocessing_numpy(chunk)])
        del chunk
        df = df_processing_cat(df)

        print(df.shape[0]/1000000,'M rows')

    c.close()
    conn.commit()

    end = time.time()

    print('import to DataFrame:','{:0.0f}'.format(end-start),'seconds')
    print('import to DataFrame:','{:0.0f}'.format((end-start)/60),'minutes')
        
    return df

def query_to_df_opt(query, conn, chunksize=500000):

    """
    Get SQL queries into DataFrames

    Parameters
    ----------
    query : str
        SQL query

    chunksize : int (optimal)
        Chunksize of read_sql_query function

    Returns
    ----------
    df : pandas.DataFrame
        df DataFrame with column optimized column types

    """
    
    df = pd.DataFrame()
    chunk = pd.DataFrame()

    for chunk in pd.read_sql_query(sql=query, con=conn, chunksize=chunksize):

        df = pd.concat([df, chunk_preprocessing_numpy(chunk)])
        del chunk
        df = df_processing_cat_opt(df)

        print(df.shape[0]/1000000,'M rows')
        
    return df