import os
import requests
import pandas as pd

JuneauLand = ['PAGY','PAHN','PAGS','PAJN','PAOH','SDIA2','PASI','PAGN','PAPG',
              'PAWG','PAKT','PAKW','PAHY','PAMM','PAFE']

JuneauMarine = ['EROA2','LIXA2','NKXA2','SCXA2','RIXA2','CSPA2', 'FFIA2',
                'PGXA2','CDEA2','LCNA2','GIXA2','MXXA2','PBPA2','GLIA2','KEXA2',
                'TKEA2']

JuneauMarineHRRR = ['EROA2','LIXA2','NKXA2','SCXA2','RIXA2','CSPA2', 'FFIA2',
                'PGXA2','CDXA2','LCNA2','GIXA2','JLXA2','PBPA2','GLIA2','KECA2',
                'TKEA2']

AnchorageLand = ['PANC','PAAQ','PAWS','PABV','SBPA2','PATO','WHMA2','PAWD',
                 'MCPA2','PAHO','PAEN','PASX','RUFA2','SHDA2','PAVD','RHVA2','RSCA2',
                 'PAGK','CXCA2','PXKA2','PLCA2','PATK','WOWA2','MMRA2','COVA2','PAII',
                 'PADL','PAPH','PADU','PAVC','PASD','UNLA2','NKLA2','PADK','PADQ',
                 'ALIA2','PANI','PAIG','PAIL','PASL','PABE','PAPM','PATG','PAKI',
                 'PAVA','PACV','ARCA2','SMCA2','PGPA2','RBTA2','GAHA2','RHRA2']

AnchorageMarine = ['PAMD','46076','46061','46060','SELA2','PILA2','AMAA2','46080',
                   '46001','46078','SKJA2','46075','PAPB','PASN','BLIA2','AUGA2']

FairbanksLand = ['ACRA2','BKCA2','TKRA2','CKNA2','SGNA2','PABI','FCPA2','TRDA2',
                 'TKLA2','PAIN','SMPA2','RXBA2','NHPA2','PAFA','TEXA2',
                 'PAEI','SLRA2','BCHA2','BREA2','PAFB','D6992','CPKA2','WICA2',
                 'PATA','PAEG','PFYU','PAGA','PABT','CHMA2','RAMA2','PAKP','IMYA2',
                 'PABA','PAAD','PASC','PAKU','PALP','PAQT','UMTA2','PAOR',
                 'PABR','PAWI','SSIA2','ASIA2','PAWN','PAOT','MNOA2','PAGH','SAGA2',
                 'PASK','PADE','HDOA2','PFSH','PFEL','PAKK','PAUN','PANV','PAOM',
                 'PATE','PATC','PAIW','PALU','PAPO','PAMK','PACZ','PASM','PADM',
                 'PATL','PAKV','PRDA2','PPIZ']

AllSites = ['PAGY','PAHN','PAGS','PAJN','PAOH','SDIA2','PASI','PAGN','PAPG',
              'PAWG','PAKT','PAKW','PAHY','PAMM','PAFE','EROA2','LIXA2','NKXA2',
              'SCXA2','RIXA2','CSPA2', 'FFIA2','PGXA2','CDEA2','LCNA2','GIXA2',
              'MXXA2','PBPA2','GLIA2','KEXA2','TKEA2','PANC','PAAQ','PAWS','PABV',
              'SBPA2','PATO','WHMA2','PAWD','MCPA2','PAHO','PAEN','PASX','RUFA2',
              'SHDA2','PAVD','RHVA2','RSCA2','PAGK','CXCA2','PXKA2','PLCA2','PATK',
              'WOWA2','MMRA2','COVA2','PAII','PADL','PAPH','PADU','PAVC','PASD',
              'UNLA2','NKLA2','PADK','PADQ','ALIA2','PANI','PAIG','PAIL','PASL',
              'PABE','PAPM','PATG','PAKI','PAVA','PACV','ARCA2','SMCA2','PGPA2',
              'RBTA2','GAHA2','RHRA2','PAMD','46076','46061','46060','SELA2',
              'PILA2','AMAA2','46080','46001','46078','SKJA2','46075','PAPB',
              'PASN','BLIA2','AUGA2','ACRA2','BKCA2','TKRA2','CKNA2','SGNA2',
              'PABI','FCPA2','TRDA2','TKLA2','PAIN','SMPA2','RXBA2','NHPA2','PAFA','TEXA2',
              'PAEI','SLRA2','BCHA2','BREA2','PAFB','D6992','CPKA2','WICA2',
              'PATA','PAEG','PFYU','PAGA','PABT','CHMA2','RAMA2','PAKP','IMYA2',
              'PABA','PAAD','PASC','PAKU','PALP','PAQT','UMTA2','PAOR',
              'PABR','PAWI','SSIA2','ASIA2','PAWN','PAOT','MNOA2','PAGH','SAGA2',
              'PASK','PADE','HDOA2','PFSH','PFEL','PAKK','PAUN','PANV','PAOM',
              'PATE','PATC','PAIW','PALU','PAPO','PAMK','PACZ','PASM','PADM',
              'PATL','PAKV','PRDA2','PPIZ']

# Define API endpoint and your API key
API_KEY = "c6c8a66a96094960aabf1fed7d07ccf0" # link to get an API key can be found at https://docs.google.com/document/d/1YuMUYog4J7DpFoEszMmFir4Ehqk9Q0GHG_QhSdrgV9M/edit?usp=sharing
BASE_URL = "https://api.synopticdata.com/v2/stations/timeseries"

output_csv = output_csv = "all_sites_wind_obs.csv"

start_date = "202501010000"
end_date = "202501202359"

# Parameters for the API request
params = {
    "token": API_KEY,
    "stid": ",".join(AllSites),  # Join site IDs into a single comma-separated string
    "vars": "wind_speed,wind_gust",  # Variables to retrieve
    "start": start_date,  # Start date (YYYYMMDDHHMM format)
    "end": end_date,    # End date (YYYYMMDDHHMM format)
    "output": "json"           # Output format
}

# Make the API request
response = requests.get(BASE_URL, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    
    # Extract station data and create a DataFrame
    stations_data = []
    for station in data["STATION"]:
        stid = station["STID"]
        wind_speeds = station["OBSERVATIONS"].get("wind_speed_set_1", [])
        wind_gusts = station["OBSERVATIONS"].get("wind_gust_set_1", [])
        times = station["OBSERVATIONS"].get("date_time", [])
        
        for time, wind_speed, wind_gust in zip(times, wind_speeds, wind_gusts):
            stations_data.append({
                "station_id": stid,
                "date_time": time,
                "wind_speed": wind_speed,
                "wind_gust": wind_gust
            })

    # Convert to DataFrame
    df = pd.DataFrame(stations_data)

    # Save to CSV
    df.to_csv(output_csv, index=False)
    print(f"Data successfully saved to {output_csv}")
else:
    print(f"Error: {response.status_code} - {response.text}")