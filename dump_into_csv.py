import requests
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions
import os
import time
#season 1955 to 2025


#Authentifaiciton by setting up an 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bigquerry-test-465502-a43bb7bb5fca.json'
#creatre an instance big querry
client = bigquery.Client()


def check_directory():
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/constructor_standings', exist_ok=True)
    os.makedirs('data/circuits', exist_ok=True)
    os.makedirs('data/constructors', exist_ok=True)
    os.makedirs('data/drivers', exist_ok=True)
    os.makedirs('data/driver_standings', exist_ok=True)
    os.makedirs('data/qualifying', exist_ok=True)
    os.makedirs('data/races', exist_ok=True)
    os.makedirs('data/results', exist_ok=True)


def upload_to_bigquerry(client, df, table_id):
    '''

    input: df + path + table_id
    
    ToDo 
    create the dataset if needed
    create the table if needed
    check duplicate 
    only upload new data 

    '''
    try:
        #get the data from the table - get all the columns
        query = f"SELECT * FROM `{table_id}`" #build the querry
        existing = client.query(query).to_dataframe() #execute the querry and format it to a dataframe
    except exceptions.NotFound:
        # Table doesn't exist yet, create empty DataFrame avec toutes les colonnes
        print("Table doesn't exist yet, will be created with first upload")
        existing = pd.DataFrame(columns=df.columns)

    # 2. Filter data in double - déduplication basée sur toutes les colonnes
    if not existing.empty: #check if existing is not empty
        # Concaténer les DataFrames pour identifier les doublons complets
        combined = pd.concat([existing, df], ignore_index=True)
        # Supprimer les doublons basés sur TOUTES les colonnes
        combined_no_duplicates = combined.drop_duplicates()
        # Garder seulement les nouvelles lignes (celles qui n'étaient pas dans 'existing')
        df = combined_no_duplicates.iloc[len(existing):]

    if not df.empty:
        # send the date to bigquerry using a job
        job = client.load_table_from_dataframe(df, table_id)
        #wait that the job is done because it's asynchone 
        job.result()
        #display a confirmation message
        print("✅ data sent")
    else:
        print('🆗 no new data to upload')

    return None
    
def handle_http_response(response, season=None, round=None):
    if response.status_code == 429:
        print("Too many request, break of 10 seconds...")
        time.sleep(10)
        return False  
    elif response.status_code != 200:
        if round:
            print(f"Error HTTP {response.status_code} for {season} round {round}")
        else:
            print(f"Error HTTP {response.status_code} for {season}")
        return False
    return True

def circuits():
    url = 'https://api.jolpi.ca/ergast/f1/circuits?limit=100'

    while True:
        response = requests.get(url)
        if handle_http_response(response):
            break
        elif response.status_code == 429:
            continue
        else:
            return

    data = response.json()
    
    df = pd.json_normalize(data['MRData']['CircuitTable']['Circuits']) #flatten the results with the normalize at the right level of the data

    useful_data = ['circuitId', 'circuitName', 'Location.lat', 'Location.long', 'Location.locality', 'Location.country'] #create a list with the data I want
    df = df[useful_data] #filter the data in the DF
    
    df = df.rename(columns={ #rename the columns to delelte the prefixe and make suitable for bigquerry
        'Location.lat': 'Location_lat', 
        'Location.long': 'Location_long',
        'Location.locality': 'Location_locality',
        'Location.country': 'Location_country'
    })

    # Convert data to the right type
    df['Location_lat'] = pd.to_numeric(df['Location_lat'], errors='coerce')
    df['Location_long'] = pd.to_numeric(df['Location_long'], errors='coerce')

    print('circuits')

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.circuits')
    return None

def constructors():
    offset = [0, 100, 200]
    df = pd.DataFrame()

    for i in offset:

        url = f'http://api.jolpi.ca/ergast/f1/constructors?limit=100&offset={i}'

        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
            
        data = response.json()
    
        temp = pd.json_normalize(data['MRData']['ConstructorTable']['Constructors']) #flatten the results with the normalize at the right level of the data
        
        useful_data = ['constructorId', 'name', 'nationality']
        temp = temp.reindex(columns=useful_data) #filter the data in the DF

        print('constructors step: ', i)

        df = pd.concat([df, temp], ignore_index=True)

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.constructors')
    return None
    
def constructor_standings():
    df = pd.DataFrame()
    df_season = pd.DataFrame()
    for season in range (1950, 2026): #2026 because the last one is escluded


        response = requests.get(f'http://api.jolpi.ca/ergast/f1/{season}/constructorstandings/')
        data = response.json()
        round_number = data['MRData']['StandingsTable'].get('round')
        if round_number is None:
            print (f'no value for season {season}')
            continue

        round_number = int(round_number)


        df_all_round = pd.DataFrame()

        for round in range (1, round_number + 1):
            url = f'http://api.jolpi.ca/ergast/f1/{season}/{round}/constructorstandings/'

            while True:
                response = requests.get(url)
                if handle_http_response(response):
                    break
                elif response.status_code == 429:
                    continue
                else:
                    return
            
              

            data = response.json()
            df_round = pd.json_normalize(
                data['MRData']['StandingsTable']['StandingsLists'], 
                record_path=['ConstructorStandings'],
                meta=['season', 'round']
            ) #flatten the results with the normalize at the right level of the data
           
            useful_data = ['season', 'round', 'Constructor.constructorId', 'position', 'positionText', 'points', 'wins']
            df_round = df_round.reindex(columns=useful_data) #filter the data in the DF

            df_round['positionText'] = df_round['positionText'].replace('-', pd.NA)

            df_round = df_round.rename(columns={ #rename the columns to delelte the prefixe and make suitable for bigquerry
                'Constructor.constructorId': 'Constructor_constructorId', 
            })

            # Convert data to the right type
            df_round['season'] = pd.to_numeric(df_round['season'], errors='coerce')
            df_round['round'] = pd.to_numeric(df_round['round'], errors='coerce')
            df_round['position'] = pd.to_numeric(df_round['position'], errors='coerce')
            df_round['points'] = pd.to_numeric(df_round['points'], errors='coerce')
            df_round['wins'] = pd.to_numeric(df_round['wins'], errors='coerce')

            print('constructor_standings: ', season, round, response) 

            df_all_round = pd.concat([df_all_round, df_round], ignore_index=True)

        df_season = pd.concat([df_season, df_all_round], ignore_index=True)

    df = pd.concat([df, df_season], ignore_index=True)

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.constructor_standings')
    return None

def drivers():
    offset = [0, 100, 200, 300, 400, 500, 600, 700, 800]
    df = pd.DataFrame()

    for i in offset:

        url = f'http://api.jolpi.ca/ergast/f1/drivers?limit=100&offset={i}'

        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
            
        data = response.json()

        temp = pd.json_normalize(data['MRData']['DriverTable']['Drivers'])

        useful_data = ['driverId', 'givenName', 'familyName', 'dateOfBirth', 'nationality']
        temp = temp[useful_data]

        temp['dateOfBirth'] = pd.to_datetime(temp['dateOfBirth'], errors='coerce')

        print('driver step : ', i)

        df = pd.concat([temp, df], ignore_index=True)

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.drivers')
    return None

def driver_standings():
    df = pd.DataFrame()
    df_season = pd.DataFrame()
    for season in range (1950, 2026): #2026 because the last one is escluded

        # this block get the number of round
        response = requests.get(f'http://api.jolpi.ca/ergast/f1/{season}/driverstandings/')
        data = response.json()
        round_number = data['MRData']['StandingsTable'].get('round')
        if round_number is None:
            print (f'no value for driver standings season {season}')
            continue

        round_number = int(round_number)

        df_all_round = pd.DataFrame()

        for round in range (1, round_number + 1):
            url = f'http://api.jolpi.ca/ergast/f1/{season}/{round}/driverstandings?limit=100'

            while True:
                response = requests.get(url)
                if handle_http_response(response):
                    break
                elif response.status_code == 429:
                    continue
                else:
                    return

            data = response.json()
            df_round = pd.json_normalize(
                data['MRData']['StandingsTable']['StandingsLists'],
                record_path=['DriverStandings'],
                meta=['season', 'round'],
                sep='_'
            )

            #get columns constructorId
            df_round['constructorId'] = df_round['Constructors'].apply(
                lambda x: x[0]['constructorId'] if isinstance(x, list) and len(x)>0 else pd.NA
            )


            useful_data = ['season', 'round', 'Driver_driverId', 'position', 'positionText', 'points', 'wins', 'constructorId']
            df_round = df_round.reindex(columns=useful_data)

            

            df_round['positionText'] = df_round['positionText'].replace('-', pd.NA) #replace the non exsitant data with NA


            df_round = df_round.rename(columns={ #rename the columns to delelte the prefixe and make suitable for bigquerry
                'Driver_driverId': 'driverId', 
            })

            #convert data to the right type 
            df_round['season'] = pd.to_numeric(df_round['season'], errors='coerce')
            df_round['round'] = pd.to_numeric(df_round['round'], errors='coerce')
            df_round['position'] = pd.to_numeric(df_round['position'], errors='coerce')
            df_round['points'] = pd.to_numeric(df_round['points'], errors='coerce')
            df_round['wins'] = pd.to_numeric(df_round['wins'], errors='coerce')

            df_all_round = pd.concat([df_all_round, df_round], ignore_index=True)
            print('driver_standings: ', season, round, response)

        df_season = pd.concat([df_season, df_all_round], ignore_index=True)
    df = pd.concat([df, df_season], ignore_index=True)
    
    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.driver_standings')
    return None

def laps():
    df = pd.DataFrame()
    
    for season in range (1996, 2026):

        url = f'http://api.jolpi.ca/ergast/f1/{season}/races/'

        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
    
        data = response.json()
        round_number = data['MRData'].get('total')

        if round_number is None:
            print (f'no value for driver standings season {season}')
            continue
        round_number = int(round_number)

        for round in range (1, round_number+1):

            url = f'http://api.jolpi.ca/ergast/f1/{season}/{round}/laps/'
    
            while True:
                response = requests.get(url)
                if handle_http_response(response):
                    break
                elif response.status_code == 429:
                    continue
                else:
                    return
                
            data = response.json()
            offset_tot = int(data['MRData']['total'])
            offset_list = [i for i in range(0, offset_tot, 100)]

            for i in offset_list:

                url = f'http://api.jolpi.ca/ergast/f1/{season}/{round}/laps?limit=100&offset={i}'
                
                while True:
                    response = requests.get(url)
                    if handle_http_response(response):
                        break
                    elif response.status_code == 429:
                        continue
                    else:
                        return
                    
                

                data = response.json()

                df_laps_offset = pd.json_normalize(
                    data['MRData']['RaceTable']['Races'], 
                    record_path=['Laps'], 
                    meta=[
                        'season', 
                        'round', 
                        'raceName', 
                        ['Circuit', 'circuitId'],
                        'date'
                    ],
                    meta_prefix= 'race_',
                    record_prefix= 'lap_', 
                    sep='_'
                )

                df_laps_offset = df_laps_offset.explode('lap_Timings')

                df_lap_time = pd.json_normalize(df_laps_offset['lap_Timings'])

                df_laps_offset = df_laps_offset.drop(columns=['lap_Timings'])

                df_laps_offset = df_laps_offset.reset_index(drop=True)
                df_lap_time = df_lap_time.reset_index(drop=True)

                df_laps_offset = pd.concat([df_laps_offset, df_lap_time], axis=1)

                useful_data = [
                    'race_season', 
                    'race_round', 
                    'race_raceName', 
                    'race_Circuit_circuitId', 
                    'race_date',
                    'lap_number',  
                    'driverId', 
                    'position', 
                    'time'
                ]

                df_laps_offset = df_laps_offset.reindex(columns=useful_data)
                
                df_laps_offset['race_season'] = pd.to_numeric(df_laps_offset['race_season'], errors='coerce')
                df_laps_offset['race_round'] = pd.to_numeric(df_laps_offset['race_round'], errors='coerce')
                df_laps_offset['race_date'] = pd.to_datetime(df_laps_offset['race_date'], errors='coerce').dt.date
                df_laps_offset['lap_number'] = pd.to_numeric(df_laps_offset['lap_number'], errors='coerce')
                df_laps_offset['position'] = pd.to_numeric(df_laps_offset['position'], errors='coerce')
                
                print('laps Season: ', season, 'round', round, 'step', i)

                df = pd.concat([df, df_laps_offset], ignore_index=True)

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.laps')
    return None 

def pitstops():
    df = pd.DataFrame()
    
    for season in range (2011, 2026):

        url = f'http://api.jolpi.ca/ergast/f1/{season}/races/'

        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
    
        data = response.json()
        round_number = data['MRData'].get('total')

        if round_number is None:
            print (f'no value for driver standings season {season}')
            continue
        round_number = int(round_number)

        for round in range (1, round_number+1):

            url = f'http://api.jolpi.ca/ergast/f1/{season}/{round}/pitstops/'
    
            while True:
                response = requests.get(url)
                if handle_http_response(response):
                    break
                elif response.status_code == 429:
                    continue
                else:
                    return
                
            data = response.json()
            offset_tot = int(data['MRData']['total'])
            offset_list = [i for i in range(0, offset_tot, 100)]

            for i in offset_list:

                url = f'http://api.jolpi.ca/ergast/f1/{season}/{round}/pitstops?limit=100&offset={i}'
                
                while True:
                    response = requests.get(url)
                    if handle_http_response(response):
                        break
                    elif response.status_code == 429:
                        continue
                    else:
                        return

                data = response.json()

                df_pitstops_offset = pd.json_normalize(
                    data['MRData']['RaceTable']['Races'], 
                    record_path=['PitStops'], 
                    meta=[
                        'season', 
                        'round', 
                        'raceName', 
                        ['Circuit', 'circuitId'],
                        'date', 
                        'time'
                    ],
                    meta_prefix= 'race_',
                    record_prefix= 'pitstops_', 
                    sep='_'
                )


                useful_data = [
                    'race_season', 
                    'race_round', 
                    'race_raceName', 
                    'race_Circuit_circuitId', 
                    'race_date',
                    'race_time',
                    'pitstops_driverId',
                    'pitstops_lap',
                    'pitstops_stop',
                    'pitstops_time',
                    'pitstops_duration'
                ]

                df_pitstops_offset = df_pitstops_offset.reindex(columns=useful_data)
                
                
                df_pitstops_offset['race_season'] = pd.to_numeric(df_pitstops_offset['race_season'], errors='coerce')
                df_pitstops_offset['race_round'] = pd.to_numeric(df_pitstops_offset['race_round'], errors='coerce')

                df_pitstops_offset['race_date'] = pd.to_datetime(df_pitstops_offset['race_date'], errors='coerce').dt.date
                df_pitstops_offset['race_time'] = pd.to_datetime(df_pitstops_offset['race_time'], errors='coerce').dt.time
            

                df_pitstops_offset['pitstops_lap'] = pd.to_numeric(df_pitstops_offset['pitstops_lap'], errors='coerce')
                df_pitstops_offset['pitstops_stop'] = pd.to_numeric(df_pitstops_offset['pitstops_stop'], errors='coerce')

                df_pitstops_offset['pitstops_time'] = pd.to_datetime(df_pitstops_offset['pitstops_time'], errors='coerce').dt.time

                print('pit stop Season: ', season, 'round', round, 'step', i)

                df = pd.concat([df, df_pitstops_offset], ignore_index=True)


    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.pitstops')
    return None
    
def qualifying():
    df = pd.DataFrame()

    url = f'http://api.jolpi.ca/ergast/f1/qualifying'

    while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
            

    data = response.json()        
    offset_tot = int(data['MRData']['total'])

    print('\n-------------\n')
    print(offset_tot)
    print('\n-------------\n')

    if offset_tot > 0:

        offset_list = [i for i in range(0, offset_tot, 100)]
        
        for i in offset_list:
            url = f'http://api.jolpi.ca/ergast/f1/qualifying?limit=100&offset={i}'

            while True:
                response = requests.get(url)
                if handle_http_response(response):
                    break
                elif response.status_code == 429:
                    continue
                else:
                    return
                
            print ('Qualifying step: ', i)

            data = response.json()

            df_season_offset = pd.json_normalize(
                data['MRData']['RaceTable']['Races'],
                record_path=['QualifyingResults'],
                meta=[
                    'season',
                    'round',
                    'raceName', 
                    'date',
                    'time',
                    ['Circuit', 'circuitId']
                ],
                record_prefix='qualifying_',
                meta_prefix='race_',
                sep='_', 
                errors='ignore'
            )

            useful_data = [
                'race_season',
                'race_round',
                'race_raceName',
                'race_Circuit_circuitId',
                'race_date',
                'race_time',
                'qualifying_number', 
                'qualifying_position',  
                'qualifying_Driver_driverId', 
                'qualifying_Constructor_constructorId', 
                'qualifying_Q1', 
                'qualifying_Q2', 
                'qualifying_Q3'
            ]
            
            df_season_offset = df_season_offset.reindex(columns=useful_data)

            # Convert data to the right type
            df_season_offset['race_season'] = pd.to_numeric(df_season_offset['race_season'], errors='coerce')
            df_season_offset['race_round'] = pd.to_numeric(df_season_offset['race_round'], errors='coerce')

            df_season_offset['race_date'] = pd.to_datetime(df_season_offset['race_date']).dt.date
            df_season_offset['race_time'] = pd.to_datetime(df_season_offset['race_time'], utc=True).dt.time

            df_season_offset['qualifying_number'] = pd.to_numeric(df_season_offset['qualifying_number'], errors='coerce')
            df_season_offset['qualifying_position'] = pd.to_numeric(df_season_offset['qualifying_position'], errors='coerce')
                                            
            df = pd.concat([df, df_season_offset], ignore_index=True)

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.qualifying')
    return None

def races():
    df = pd.DataFrame()
    url = f'http://api.jolpi.ca/ergast/f1/races/'

    while True:
        response = requests.get(url)
        if handle_http_response(response):
            break
        elif response.status_code == 429:
            continue
        else:
            return
        
    data = response.json()        
    offset_tot = int(data['MRData']['total'])

    offset_list = [i for i in range(0, offset_tot, 100)]

    for i in offset_list:

        url = f'http://api.jolpi.ca/ergast/f1/races?limit=100&offset={i}'

        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
            
        print('Races step', i)

        data = response.json()

        df_races_offset = pd.json_normalize(
            data['MRData']['RaceTable']['Races'],
            sep='_'
        )

        useful_data = [
            'season', 
            'round', 
            'raceName', 
            'Circuit_circuitId', 
            'date'
        ]

        df_races_offset = df_races_offset.reindex(columns=useful_data)

        df_races_offset['season'] = pd.to_numeric(df_races_offset['season'], errors='coerce')
        df_races_offset['round'] = pd.to_numeric(df_races_offset['round'], errors='coerce')
        df_races_offset['date'] = pd.to_datetime(df_races_offset['date']).dt.date


        df = pd.concat([df, df_races_offset], ignore_index=True)
  
    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.races')
    return None 

def results():
    df = pd.DataFrame()
    url = f'http://api.jolpi.ca/ergast/f1/results/'

    while True:
        response = requests.get(url)
        if handle_http_response(response):
            break
        elif response.status_code == 429:
            continue
        else:
            return
        
    data = response.json()        
    offset_tot = int(data['MRData']['total'])

    offset_list = [i for i in range(0, offset_tot, 100)]

    for i in offset_list:

        url = f'http://api.jolpi.ca/ergast/f1/results?limit=100&offset={i}'

        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
            
        print('Results step', i)

        data = response.json()

        df_races_offset = pd.json_normalize(
            data['MRData']['RaceTable']['Races'],
            record_path=['Results'],
            meta = [
                'season', 
                'round', 
                'raceName', 
                ['Circuit', 'circuitId'], 
                'date'
            ],
            record_prefix='results_',
            meta_prefix= 'race_',
            sep='_'
        )

        useful_data = [
            'race_season', 
            'race_round',
            'race_raceName',
            'race_Circuit_circuitId',
            'race_date', 
            'results_number',
            'results_position', 
            'results_positionText',
            'results_points',
            'results_Driver_driverId', 
            'results_Constructor_constructorId', 
            'results_grid',
            'results_laps',
            'results_status',
            'results_Time_time',
            'results_Time_millis'
        ]

        df_races_offset = df_races_offset.reindex(columns=useful_data)

        #conert the data to the righ type 
        df_races_offset['race_season'] = pd.to_numeric(df_races_offset['race_season'], errors='coerce')
        df_races_offset['race_round'] = pd.to_numeric(df_races_offset['race_round'], errors='coerce')

        df_races_offset['race_date'] = pd.to_datetime(df_races_offset['race_date']).dt.date

        df_races_offset['results_number'] = pd.to_numeric(df_races_offset['results_number'], errors='coerce')
        df_races_offset['results_position'] = pd.to_numeric(df_races_offset['results_position'], errors='coerce')
        df_races_offset['results_points'] = pd.to_numeric(df_races_offset['results_points'], errors='coerce')
        df_races_offset['results_grid'] = pd.to_numeric(df_races_offset['results_grid'], errors='coerce')
        df_races_offset['results_laps'] = pd.to_numeric(df_races_offset['results_laps'], errors='coerce')

        df = pd.concat([df, df_races_offset], ignore_index=True)
  
    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.results')
    return None 

def sprint():
    df = pd.DataFrame()

    url = f'http://api.jolpi.ca/ergast/f1/sprint/'

    while True:
        response = requests.get(url)
        if handle_http_response(response):
            break
        elif response.status_code == 429:
            continue
        else:
            return
        
    data = response.json()
    offset_tot = int(data['MRData']['total'])
    offset_list = [i for i in range(0, offset_tot, 100)]

    for i in offset_list:

        url = f'http://api.jolpi.ca/ergast/f1/sprint?limit=100&offset={i}'
        
        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
            
        print('sprint Season:',  'step', i)

        data = response.json()

        df_sprint_offset = pd.json_normalize(
            data['MRData']['RaceTable']['Races'], 
            record_path=['SprintResults'], 
            meta=[
                'season', 
                'round', 
                'raceName', 
                ['Circuit', 'circuitId'],
                'date', 
                'time'
            ],
            meta_prefix= 'race_',
            record_prefix= 'SprintResults_', 
            sep='_'
        )

        
        useful_data = [
            'race_season', 
            'race_round', 
            'race_raceName', 
            'race_Circuit_circuitId', 
            'race_date',
            'race_time',
            'SprintResults_number', 
            'SprintResults_position', 
            'SprintResults_positionText', 
            'SprintResults_points', 
            'SprintResults_Driver_driverId', 
            'SprintResults_Constructor_constructorId', 
            'SprintResults_grid', 
            'SprintResults_laps', 
            'SprintResults_status', 
            'SprintResults_Time_time', 
            'SprintResults_Time_millis', 
            'SprintResults_FastestLap_lap', 
            'SprintResults_FastestLap_Time_time'
        ]
        
        df_sprint_offset = df_sprint_offset.reindex(columns=useful_data)
        
        df_sprint_offset['race_season'] = pd.to_numeric(df_sprint_offset['race_season'], errors='coerce')
        df_sprint_offset['race_round'] = pd.to_numeric(df_sprint_offset['race_round'], errors='coerce')

        df_sprint_offset['race_date'] = pd.to_datetime(df_sprint_offset['race_date'], errors='coerce').dt.date
        df_sprint_offset['race_time'] = pd.to_datetime(df_sprint_offset['race_time'], errors='coerce').dt.time

        df_sprint_offset['SprintResults_number'] = pd.to_numeric(df_sprint_offset['SprintResults_number'], errors='coerce')
        df_sprint_offset['SprintResults_position'] = pd.to_numeric(df_sprint_offset['SprintResults_position'], errors='coerce')
        df_sprint_offset['SprintResults_points'] = pd.to_numeric(df_sprint_offset['SprintResults_points'], errors='coerce')
        df_sprint_offset['SprintResults_grid'] = pd.to_numeric(df_sprint_offset['SprintResults_grid'], errors='coerce')
        df_sprint_offset['SprintResults_laps'] = pd.to_numeric(df_sprint_offset['SprintResults_laps'], errors='coerce')
        df_sprint_offset['SprintResults_FastestLap_lap'] = pd.to_numeric(df_sprint_offset['SprintResults_FastestLap_lap'], errors='coerce')

        df = pd.concat([df, df_sprint_offset], ignore_index=True)

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.sprint')
    return None 

def status():
    df = pd.DataFrame()

    url = f'http://api.jolpi.ca/ergast/f1/status/'

    while True:
        response = requests.get(url)
        if handle_http_response(response):
            break
        elif response.status_code == 429:
            continue
        else:
            return
        
    data = response.json()
    offset_tot = int(data['MRData']['total'])
    offset_list = [i for i in range(0, offset_tot, 100)]

    for i in offset_list:

        url = f'http://api.jolpi.ca/ergast/f1/status/?limit=100&offset={i}'
        
        while True:
            response = requests.get(url)
            if handle_http_response(response):
                break
            elif response.status_code == 429:
                continue
            else:
                return
            
        print('status: step', i)

        data = response.json()

        df_status_offset = pd.json_normalize(
            data['MRData']['StatusTable']['Status'], 
            sep='_'
        )
        print('\n------------\n')
        print(df_status_offset.columns)
        print('\n------------\n')

        
        df_status_offset['statusId'] = pd.to_numeric(df_status_offset['statusId'], errors='coerce')
        df_status_offset['count'] = pd.to_numeric(df_status_offset['count'], errors='coerce')
        

        df = pd.concat([df, df_status_offset], ignore_index=True)

    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.status')
    return None 


def test():
    return None

def main():
    check_directory()

    circuits()

    constructors()
    constructor_standings()
    drivers()
    driver_standings()
    laps()
    pitstops()
    qualifying()
    races()
    results()
    sprint()
    status()

if __name__ == "__main__":
    main()