#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from flask import Flask, request, jsonify
from flask_cors import CORS
import dask.dataframe as dd
import s3fs
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
CORS(app)

# Set up AWS S3 connection
fs = s3fs.S3FileSystem()
bucket_name = '608project'

# Define borough mapping
borough_mapping = {
    'MANHATTAN': 'M',
    'BRONX': 'B',
    'BROOKLYN': 'K',
    'STATEN ISLAND': 'S',
    'QUEENS': 'Q'
}

executor = ThreadPoolExecutor(max_workers=8)

def load_data(year):
    file_path = f's3://{bucket_name}/nypd_cleaned_{year}.parquet'
    try:
        data = dd.read_parquet(file_path, filesystem=fs)
        print(f"Loaded data for year {year}, columns: {data.columns}")
        return data
    except Exception as e:
        print(f"Error loading data for year {year}: {e}")
        return dd.from_pandas(pd.DataFrame(), npartitions=1)  # Return an empty dataframe if an error occurs

def filter_data(data, boroughs, offenses, ethnicities, genders, age_categories):
    try:
        filtered = data[
            (data['ARREST_BORO'].isin(boroughs)) &
            (data['OFNS_DESC'].isin(offenses)) &
            (data['PERP_RACE'].isin(ethnicities)) &
            (data['PERP_SEX'].isin(genders)) &
            (data['AGE_GROUP'].isin(age_categories))
        ]
        return filtered
    except KeyError as e:
        print(f"KeyError during filtering: {e}")
        raise

@app.route('/filter', methods=['POST'])
def filter_data_endpoint():
    req = request.json
    years = req.get('years', [])
    boroughs = req.get('boroughs', [])
    offenses = req.get('offenses', [])
    ethnicities = req.get('ethnicities', [])
    genders = req.get('genders', [])
    age_categories = req.get('age_categories', [])

    # convert borough names to codes
    borough_codes = [borough_mapping.get(borough, borough) for borough in boroughs]

    # Will load data for the specified years in parallel
    futures = [executor.submit(load_data, year) for year in years]
    data_frames = [future.result() for future in as_completed(futures)]
    data = dd.concat(data_frames)

    print(f"Columns after concatenation: {data.columns}")

    try:
        filtered_data = filter_data(data, borough_codes, offenses, ethnicities, genders, age_categories).compute()
        return jsonify(filtered_data[['ARREST_DATE','ARREST_BORO', 'OFNS_DESC', 'PERP_RACE', 'PERP_SEX', 'AGE_GROUP', 'Latitude', 'Longitude']].to_dict(orient='records'))
    except Exception as e:
        print(f"Error during filtering: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/getdata', methods=['GET'])
def get_data():
    year = request.args.get('year')
    
    if not year:
        return jsonify({'error': 'Year parameter is required'}), 400

    try:
        data = load_data(year).compute()
        return jsonify(data[['ARREST_DATE','ARREST_BORO', 'OFNS_DESC', 'PERP_RACE', 'PERP_SEX', 'AGE_GROUP', 'Latitude', 'Longitude']].to_dict(orient='records'))
    except Exception as e:
        print(f"Error fetching data for year {year}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data-summary', methods=['GET'])
def data_summary():
    year = request.args.get('year')
    if not year:
        return jsonify({'error': 'Year parameter is required'}), 400

    try:
        data = load_data(year).compute()
        summary = {
            'total_records': len(data),
            'boroughs': data['ARREST_BORO'].unique().tolist(),
            'offenses': data['OFNS_DESC'].unique().tolist(),
            'ethnicities': data['PERP_RACE'].unique().tolist(),
            'genders': data['PERP_SEX'].unique().tolist(),
            'age_categories': data['AGE_GROUP'].unique().tolist()
        }
        return jsonify(summary)
    except Exception as e:
        print(f"Error summarizing data for year {year}: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

