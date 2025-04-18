import pandas as pd 
import requests
from google.cloud import storage
from datetime import datetime


def run_etl():
    url = "https://realty-us.p.rapidapi.com/properties/search-rent"
    csv_filename = 'data.csv'

    for cnt in list(range(1,111)):
        querystring = {"location":"state:New York, NY","resultsPerPage":"200","page":cnt,"sortBy":"relevance"}

        headers = {
            "x-rapidapi-key": "Your_API_KEY",
            "x-rapidapi-host": "realty-us.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)
        master_data = response.json()

        property_ids_lst = []
        city_lst = []
        postal_lst = []
        state_code_lst = []
        state_lst = []
        country_lst = []
        latitude_lst = []
        longitude_lst = []
        baths_lst = []
        beds_lst = []
        sqft_lst = []
        types_lst = []
        list_price_lst = []
        list_date_lst = []
        last_sold_date_lst = []
        last_sold_price_lst = []

        for itm in master_data['data']['results']:
            # print(itm)
            property_ids_lst.append(itm['property_id'])
            city_lst.append(itm['location']['address']['city'])
            postal_lst.append(itm['location']['address']['postal_code'])
            state_code_lst.append(itm['location']['address']['state_code'])
            state_lst.append(itm['location']['address']['state'])
            country_lst.append(itm['location']['address']['country'])
            if itm['location']['address']['coordinate']:
                latitude_lst.append(itm['location']['address']['coordinate']['lat'])
                longitude_lst.append(itm['location']['address']['coordinate']['lon'])
            else:
                latitude_lst.append(None)
                longitude_lst.append(None)
            baths_lst.append(itm['description']['baths'])
            beds_lst.append(itm['description']['beds'])
            sqft_lst.append(itm['description']['sqft'])
            types_lst.append(itm['description']['type'])
            list_price_lst.append(itm['list_price'])
            list_date_lst.append(itm['list_date'])
            last_sold_date_lst.append(itm['last_sold_date'])
            last_sold_price_lst.append(itm['last_sold_price'])
    
        df = pd.DataFrame({
            "property_id": property_ids_lst,
            "city": city_lst,
            "postal_code": postal_lst,
            "state_code": state_code_lst,
            "state": state_lst,
            "country": country_lst,
            "latitude": latitude_lst,
            "longitude": longitude_lst,
            "beds": beds_lst,
            "baths": baths_lst,
            "sqft": sqft_lst,
            "property_type": types_lst,
            "list_price": list_price_lst,
            "list_date": list_date_lst,
            "last_sold_date": last_sold_date_lst,
            "last_sold_price": last_sold_price_lst    
        })

        # Get the current timestamp
        current_timestamp = datetime.now()

        # Format the timestamp as "yyyy-mm-dd HH24:mi:ss"
        formatted_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

        df['create_on'] = formatted_timestamp

        if(cnt == 1):
            df.to_csv(csv_filename, index=False, header=None)
        else:
            df.to_csv(csv_filename, index=False, header=None, mode='a+')
            
        print(f"page {cnt} completed!")
    

    # Upload the CSV file to GCS
    bucket_name = 'demo-etl'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination_blob_name = f'real-estate-src/{csv_filename}'  # The path to store in GCS

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_filename)

    print(f"File {csv_filename} uploaded to GCS bucket {bucket_name}.")

run_etl()