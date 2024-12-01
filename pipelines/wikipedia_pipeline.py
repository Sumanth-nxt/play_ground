import json
import time
import pandas as pd
import requests
from opencage.geocoder import OpenCageGeocode
from bs4 import BeautifulSoup
from io import StringIO

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    tables = soup.find_all('table', class_='wikitable')

    all_dfs = []
    for i, table in enumerate(tables):
        df = pd.read_html(StringIO(str(table)))[0]
        df['Table'] = f"Table_{i + 1}"
        all_dfs.append(df)
    combined_df = pd.concat(all_dfs, ignore_index=True)

    combined_df['rank'] = range(1, len(combined_df) + 1)
    if 'Ground' in combined_df.columns:
        combined_df['ground'] = combined_df['Ground'].apply(clean_text)

    if 'Capacity' in combined_df.columns:
        combined_df['capacity'] = combined_df['Capacity'].apply(
            lambda x: clean_text(x).replace(',', '').replace('.', ''))

    if 'City' in combined_df.columns:
        combined_df['city'] = combined_df['City'].apply(clean_text)

    if 'Country' in combined_df.columns:
        combined_df['country'] = combined_df['Country'].apply(clean_text)

    if 'Home team(s)' in combined_df.columns:
        combined_df['home_team'] = combined_df['Home team(s)'].str.strip()

    all_data = combined_df.to_dict(orient='records')

    json_rows = json.dumps(all_data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"

api_key = '****'
geocoder = OpenCageGeocode(api_key)

last_request_time = 0

def get_lat_long(country, city):
    global last_request_time
    query = f'{city}, {country}'
    current_time = time.time()
    time_diff = current_time - last_request_time
    if time_diff < 2:
        time.sleep(2 - time_diff)
    result = geocoder.geocode(query)
    last_request_time = time.time()
    if result:
        location = result[0]['geometry']
        return location['lat'], location['lng']
    return None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    grounds_df = pd.DataFrame(data)
    grounds_df['location'] = grounds_df.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    grounds_df['images'] = grounds_df['Image'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    #grounds_df['capacity'] = grounds_df['capacity'].astype(int) # Value has a range so add error handling here

    # handle the duplicates
    duplicates = grounds_df[grounds_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    grounds_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=grounds_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('ground_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    data.to_csv('data/' + file_name, index=False)