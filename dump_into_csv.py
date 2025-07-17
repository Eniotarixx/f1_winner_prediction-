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

url = [
    '/ergast/f1/circuits/',
    '/ergast/f1/constructors/',
    '/ergast/f1/{season}/constructorstandings/',
    '/ergast/f1/drivers/', 
    '/ergast/f1/{season}/driverstandings/', 
    '/ergast/f1/{season}/{round}/laps/',
    '/ergast/f1/{season}/{round}/pitstops/',
    '/ergast/f1/{season}/qualifying/',
    '/ergast/f1/races/',
    '/ergast/f1/results/',
    '/ergast/f1/seasons/',
    '/ergast/f1/sprint/',
    '/ergast/f1/status/']

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
            
            print('constructor_standings: ', season, round, response)   

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
            
        print('driver step : ', i)
            
        data = response.json()

        temp = pd.json_normalize(data['MRData']['DriverTable']['Drivers'])

        useful_data = ['driverId', 'givenName', 'familyName', 'dateOfBirth', 'nationality']
        temp = temp[useful_data]

        temp['dateOfBirth'] = pd.to_datetime(temp['dateOfBirth'], errors='coerce')

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
                
            print('driver_standings: ', season, round, response)
            print('\n-----------\n')

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
        df_season = pd.concat([df_season, df_all_round], ignore_index=True)
    df = pd.concat([df, df_season], ignore_index=True)
    
    upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.driver_standings')
    return None

def laps():
    return None

def pitstops():
    return None
    
def qualifying():
    for season in range (1950, 2026): #2026 because the last one is escluded
        round = 1 
        has_more_races = True
        while has_more_races:
            if not os.path.isfile(f'data/qualifying/{season}_{round}.csv'):
                response = requests.get(f'http://api.jolpi.ca/ergast/f1/{season}/{round}/qualifying/')

                if not handle_http_response(response, season, round):
                    break

                print('qualifying: ', season, round, response)
                data = response.json()
                qualifying = data['MRData']['RaceTable']['Races']

                has_more_races = bool(qualifying)
                if has_more_races:

                    rows = [] # create a row for each qualifying reslut otherwise the last entry overwrite the precedent one
                    for q in qualifying:
                        circuit_info = {
                            'season' : q['season'],
                            'round' : q['round'],
                            'raceName' : q['raceName'],
                            'circuitName' : q['Circuit']['circuitName'],
                            'country' : q['Circuit']['Location']['country'], 
                            'date' : q['date'],
                            'time': q.get('time', None)
                        }

                        for r in q['QualifyingResults']:
                            row = circuit_info.copy()
                            row['number'] = r['number']
                            row['position'] = r['position']
                            row['driverId'] = r['Driver']['driverId']
                            row['permanentNumber'] = r['Driver'].get('permanentNumber', None)
                            row['code'] = r['Driver'].get('code', None)
                            row['givenName'] = r['Driver']['givenName']
                            row['familyName'] = r['Driver']['familyName']
                            row['dateOfBirth'] = r['Driver']['dateOfBirth']
                            row['driver_nationality'] = r['Driver']['nationality']
                            row['constructorId'] = r['Constructor']['constructorId']
                            row['constructor_name'] = r['Constructor']['name']
                            row['constructor_nationality'] = r['Constructor']['nationality']
                            row['Q1'] = r.get('Q1', None)
                            row['Q2'] = r.get('Q2', None)
                            row['Q3'] = r.get('Q3', None)
                            rows.append(row)

                    df = pd.DataFrame(rows)

                    if not df.empty:
                        upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.qualifying')
                    else:
                        print('no new data to upload')
                    round +=1
    return None

def races():
    for season in range (1950, 2026): #2026 because the last one is escluded
        if not os.path.isfile(f'data/races/{season}.csv'):
            response = requests.get(f'https://api.jolpi.ca/ergast/f1/{season}/races/')

            if not handle_http_response(response, season):
                break

            print('races: ', season, response)
            data = response.json()
            races = data['MRData']['RaceTable']['Races']
            for r in races:
                r['circuitId'] = r['Circuit']['circuitId']
                r['circuitName'] = r['Circuit']['circuitName']

                r['first_practice_date'] = r.get('FirstPractice', {}).get('date', None)
                r['first_practice_time'] = r.get('FirstPractice', {}).get('time', None)

                r['second_practice_date'] = r.get('SecondPractice', {}).get('date', None)
                r['second_practice_time'] = r.get('SecondPractice', {}).get('time', None)        

                r['third_practice_date'] = r.get('ThirdPractice', {}).get('date', None)
                r['third_practice_time'] = r.get('ThirdPractice', {}).get('time', None) 

                r['qualifying_date'] = r.get('Qualifying', {}).get('date', None)
                r['qualifying_time'] = r.get('Qualifying', {}).get('time', None)

                r['sprint_qualifying_date'] = r.get('SprintQualifying', {}).get('date', None)
                r['sprint_qualifying_time'] = r.get('SprintQualifying', {}).get('time', None) 

                r['sprint_date'] = r.get('Sprint', {}).get('date', None)
                r['sprint_time'] = r.get('Sprint', {}).get('time', None) 

                del r['Circuit']
                r.pop('FirstPractice', None)
                r.pop('SecondPractice', None)
                r.pop('ThirdPractice', None)
                r.pop('Qualifying', None)
                r.pop('SprintQualifying', None)
                r.pop('Sprint', None)

            df = pd.DataFrame(races)

            if not df.empty:
                upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.races')
            else:
                print('no new data to upload')

    return None

def results():
    for season in range (1950, 2026): #2026 because the last one is escluded
        round = 1 
        has_more_races = True
        while has_more_races:
            if not os.path.isfile(f'data/results/{season}_{round}.csv.csv'):
                response = requests.get(f'https://api.jolpi.ca/ergast/f1/{season}/{round}/results/')

                if not handle_http_response(response, season, round):
                    break

                print('results: ', season, round, response)
                data = response.json()
                results = data['MRData']['RaceTable']['Races']

                has_more_races = bool(results)
                if has_more_races:
                        
                    rows = [] # create row for each driver

                    for i in results:
                        circuit_info = {
                            'season' : i['season'],
                            'round' : i['round'],
                            'raceName' : i['raceName'],
                            'circuitName' : i['Circuit']['circuitName'],
                            'country' : i['Circuit']['Location']['country'], 
                            'date' : i['date'],
                            'time': i.get('time', None)
                            }
                        
                        for j in i['Results']:
                            row = circuit_info.copy()
                            row['number'] = j['number']
                            row['position'] = j['position']
                            row['points'] = j['points']

                            row['driverId'] = j['Driver']['driverId']
                            row['permanentNumber'] = j['Driver'].get('permanentNumber', None)
                            row['code'] = j['Driver'].get('code', None)
                            row['givenName'] = j['Driver']['givenName']
                            row['familyName'] = j['Driver']['familyName']
                            row['dateOfBirth'] = j['Driver']['dateOfBirth']
                            row['driver_nationality'] = j['Driver']['nationality']

                            row['constructorId'] = j['Constructor']['constructorId']
                            row['constructor_name'] = j['Constructor']['name']
                            row['constructor_nationality'] = j['Constructor']['nationality']

                            row['grid'] = j['grid']
                            row['laps'] = j['laps']
                            row['status'] = j['status']

                            row['time'] = j.get('Time', {}).get('time', None)
                            row['time_millis'] = j.get('Time', {}).get('millis', None)

                            row['fastest_lap_rank'] = j.get('FastestLap', {}).get('rank', None)
                            row['fastest_lap_lap'] = j.get('FastestLap', {}).get('lap', None)
                            row['fastest_lap_time'] = j.get('FastestLap', {}).get('Time', {}).get('time', None)
                            row['fastest_lap_avg_speed'] = j.get('FastestLap', {}).get('AverageSpeed', {}).get('speed', None)
                            row['fastest_lap_avg_speed_units'] = j.get('FastestLap', {}).get('AverageSpeed', {}).get('units', None)

                            rows.append(row)
                        
                        df = pd.DataFrame(rows)

                        if not df.empty:
                            upload_to_bigquerry(client, df, f'bigquerry-test-465502.f1_data.results')
                        else:
                            print('no new data to upload')
            
                        round +=1

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
    #test()

if __name__ == "__main__":
    main()