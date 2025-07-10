import requests
import pandas as pd
import os
import time
#season 1955 to 2025

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
    
def handle_http_response(response, season, round=None):
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
    for season in range (1950, 2026): #2026 because the last one is escluded
        if not os.path.isfile(f'data/circuits/{season}.csv'):
            response = requests.get(f'https://api.jolpi.ca/ergast/f1/{season}/circuits/')

            if not handle_http_response(response, season):
                break

            print('circuits: ', season, response)
            data = response.json()
            circuits = data['MRData']['CircuitTable']['Circuits']
            if circuits:
                for c in circuits:
                    c['lat'] = c['Location']['lat']
                    c['long'] = c['Location']['long']
                    c['locality'] = c['Location']['locality']
                    c['country'] = c['Location']['country']
                    del c['Location']
                df = pd.DataFrame(circuits)
                df.to_csv(f'data/circuits/{season}.csv', index=False)
                time.sleep(1)
    return None

def constructors():
    for season in range (1950, 2026): #2026 because the last one is escluded
        if not os.path.isfile(f'data/constructors/{season}.csv'):
            response = requests.get(f'https://api.jolpi.ca/ergast/f1/{season}/constructors/')

            if not handle_http_response(response, season):
                break

            print('constructors: ', season, response)
            data = response.json()
            constructors = data['MRData']['ConstructorTable']['Constructors']
            df = pd.DataFrame(constructors)
            df.to_csv(f'data/constructors/{season}.csv', index=False)
            time.sleep(1)
    return None
    
def constructor_standings():
    for season in range (1950, 2026): #2026 because the last one is escluded
        if not os.path.isfile(f'data/constructor_standings/constructorstandings_{season}.csv'):
            response = requests.get(f'http://api.jolpi.ca/ergast/f1/{season}/constructorstandings/')

            if not handle_http_response(response, season):
                break

            print('constructor_standings: ', season, response)
            data = response.json()
            standings_lists =data['MRData']['StandingsTable']['StandingsLists']
            if standings_lists: #check if there's at least one element
                constructor_standings = standings_lists[0]['ConstructorStandings']
                for c in constructor_standings:
                    c['constructorId'] = c['Constructor']['constructorId']
                    c['url'] = c['Constructor']['url']
                    c['name'] = c['Constructor']['name']
                    c['nationality'] = c['Constructor']['nationality']
                    del c['Constructor']
                    c['season'] = standings_lists[0]['season']
                    c['round'] = standings_lists[0]['round']
                df = pd.DataFrame(constructor_standings)
                df.to_csv(f'data/constructor_standings/constructorstandings_{season}.csv', index=False)
                time.sleep(1)
    return None

def drivers():
    for season in range (1950, 2026): #2026 because the last one is escluded
        if not os.path.isfile(f'data/drivers/{season}.csv'):
            response = requests.get(f'http://api.jolpi.ca/ergast/f1/{season}/drivers/')

            if not handle_http_response(response, season):
                break

            print('drivers: ', season, response)
            data = response.json()
            drivers = data['MRData']['DriverTable']['Drivers']
            df = pd.DataFrame(drivers)
            df.to_csv(f'data/drivers/{season}.csv', index=False)
            time.sleep(1)
    return None

def driver_standings():
    for season in range (1950, 2026): #2026 because the last one is escluded
        if not os.path.isfile(f'data/driver_standings/{season}.csv'):
            response = requests.get(f'http://api.jolpi.ca/ergast/f1/{season}/driverstandings/')

            if not handle_http_response(response, season):
                break

            print('driver_standings: ', season, response)
            data = response.json()
            standings_list = data['MRData']['StandingsTable']['StandingsLists']
            driver_standings = standings_list[0]['DriverStandings']
            for d in driver_standings:
                d['driverId'] = d['Driver']['driverId']
                d['permanentNumber'] = d['Driver'].get('permanentNumber', None)
                d['code'] = d['Driver'].get('code', None)
                d['url'] = d['Driver']['url']
                d['givenName'] = d['Driver']['givenName']
                d['familyName'] = d['Driver']['familyName']
                d['dateOfBirth'] = d['Driver']['dateOfBirth']
                d['nationality'] = d['Driver']['nationality']
                del d['Driver']
                d['constructorId'] = d['Constructors'][0]['constructorId']
                d['cons_url'] = d['Constructors'][0]['url']
                d['cons_name'] = d['Constructors'][0]['name']
                d['cons_nationality'] = d['Constructors'][0]['nationality']
                del d['Constructors']
            df = pd.DataFrame(driver_standings)
            df.to_csv(f'data/driver_standings/{season}.csv', index=False)
            time.sleep(1)
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
                    df.to_csv(f'data/qualifying/{season}_{round}.csv', index=False)
                    round +=1
                    time.sleep(1)
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
            df.to_csv(f'data/races/{season}.csv', index=False)
            time.sleep(1)
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
                        df.to_csv(f'data/results/{season}_{round}.csv', index=False)
                        round +=1

                        time.sleep(1)
    return None


def test():
    return None

def main ():
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