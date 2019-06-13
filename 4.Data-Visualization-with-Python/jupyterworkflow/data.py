def get_url(start_year, end_year):

    """
    Create url list and filepath list

    Parameters
    ----------
    start_year : int
        the first year to start the download range
    end_year : int
        the last year to end the download range

    Returns
    ----------
    url : list of str
        List with complete url from the start_year to end_year
    filepath : list of str
        List with the complete filepath where files will be downloaded
    """
    
    # Create lists
    url, filepath = [], []
    
    for year in range(start_year,end_year+1):
    
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
        List with complete url from the start_year to end_year
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

    # Import packages used on this function
    import os
    import time
    import requests
    import bz2
    
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
        List with complete url from the start_year to end_year
    filepath : list of str
        List with the complete filepath where files will be downloaded

    Returns
    ----------

    """

    import os
    
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