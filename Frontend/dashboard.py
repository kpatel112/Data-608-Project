#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import logging
import concurrent.futures

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("New York Crime Data Dashboard"),
    dcc.Dropdown(
        id='year-dropdown',
        options=[{'label': str(year), 'value': year} for year in range(2006, 2024)] + [{'label': 'Select All', 'value': 'all'}],
        value=[2023],  # Default value
        multi=True,
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='borough-dropdown',
        options=[{'label': borough, 'value': borough} for borough in ['MANHATTAN', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND']] + [{'label': 'Select All', 'value': 'all'}],
        value=['MANHATTAN'],  # Default value
        multi=True,
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='offense-dropdown',
        options=[{'label': offense, 'value': offense} for offense in [
            'VIOLENT CRIMES', 'DRUG-RELATED OFFENSES', 'ASSAULT', 'THEFT',
            'CRIMINAL TRESPASS', 'SEXUAL OFFENSES', 'ADMIN AND STATE LAWS', 
            'WEAPONS', 'PUBLIC ORDER', 'FRAUD AND FORGERY', 'CRIMINAL MISCHIEF & RELATED OF',
            'PUBLIC SAFETY AND MORALITY', 'TRAFFIC OFFENSES', 'HARRASSMENT 2', '(null)',
            'OTHER OFFENSES RELATED TO THEF', 'MISC OFFENSES', 'KIDNAPPING & RELATED OFFENSES', 
            'LOITERING/GAMBLING (CARDS, DIC', 'CRIMINAL MISCHIEF & RELATED OFFENSES', 'HARASSMENT',
            'UNAUTHORIZED USE OF A VEHICLE 3 (UUV)'
        ]] + [{'label': 'Select All', 'value': 'all'}],
        value=['VIOLENT CRIMES'],  # Default value
        multi=True,
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='ethnicity-dropdown',
        options=[{'label': ethnicity, 'value': ethnicity} for ethnicity in [
            'BLACK', 'WHITE', 'WHITE HISPANIC', 'BLACK HISPANIC',
            'ASIAN / PACIFIC ISLANDER', 'UNKNOWN', 'OTHER',
            'AMERICAN INDIAN/ALASKAN NATIVE'
        ]] + [{'label': 'Select All', 'value': 'all'}],
        value=['BLACK'],  # Default value
        multi=True,
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='gender-dropdown',
        options=[{'label': gender, 'value': gender} for gender in ['M', 'F', 'U']] + [{'label': 'Select All', 'value': 'all'}],
        value=['M'],  # Default value
        multi=True,
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='age-category-dropdown',
        options=[{'label': age, 'value': age} for age in [
            '25-44', '45-64', '18-24', '<18', '65+'
        ]] + [{'label': 'Select All', 'value': 'all'}],
        value=['25-44'],  # Default value
        multi=True,
        style={'width': '50%'}
    ),
    html.Button('Apply', id='apply-button', n_clicks=0),
    dcc.Graph(id='bar-graph'),
    dcc.Graph(id='line-plot'),
    dcc.Graph(id='map-plot',style={'height': '800px', 'width': '100%'}),
    html.Div(id='output-container')
])

# borough mapping dictionary
borough_mapping = {
    'MANHATTAN': 'M',
    'BRONX': 'B',
    'BROOKLYN': 'K',
    'QUEENS': 'Q',
    'STATEN ISLAND': 'S'
}

# function to fetch data
def fetch_data(params):
    try:
        response = requests.post("http://backend:5000/filter", json=params)
        response.raise_for_status()
        data = pd.DataFrame(response.json())
        return data
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return pd.DataFrame()

# function to aggregate data
def aggregate_data(data, selected_years):
    if 'ARREST_DATE' in data.columns:
        data['ARREST_DATE'] = pd.to_datetime(data['ARREST_DATE'])
        data = data[data['ARREST_DATE'].dt.year.isin(selected_years)]
        if 'crime_count' in data.columns:
            data_aggregated = data.groupby(['ARREST_DATE', 'ARREST_BORO']).agg({'crime_count': 'sum'}).reset_index()
        else:
            data_aggregated = data.groupby(['ARREST_DATE', 'ARREST_BORO']).size().reset_index(name='crime_count')
    else:
        data_aggregated = pd.DataFrame()

    return data_aggregated

@app.callback(
    [Output('bar-graph', 'figure'),
     Output('line-plot', 'figure'),
     Output('map-plot', 'figure')],
    [Input('apply-button', 'n_clicks')],
    [
        State('year-dropdown', 'value'),
        State('borough-dropdown', 'value'),
        State('offense-dropdown', 'value'),
        State('ethnicity-dropdown', 'value'),
        State('gender-dropdown', 'value'),
        State('age-category-dropdown', 'value')
    ]
)
def update_output(n_clicks, selected_years, selected_boroughs, selected_offenses, selected_ethnicities, selected_genders, selected_age_categories):
    logger.debug("Callback triggered")
    if n_clicks > 0:
        if 'all' in selected_years:
            selected_years = list(range(2006, 2024))
        if 'all' in selected_boroughs:
            selected_boroughs = ['MANHATTAN', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND']
        if 'all' in selected_offenses:
            selected_offenses = [
                'VIOLENT CRIMES', 'DRUG-RELATED OFFENSES', 'ASSAULT', 'THEFT',
                'CRIMINAL TRESPASS', 'SEXUAL OFFENSES', 'ADMIN AND STATE LAWS', 
                'WEAPONS', 'PUBLIC ORDER', 'FRAUD AND FORGERY', 'CRIMINAL MISCHIEF & RELATED OF',
                'PUBLIC SAFETY AND MORALITY', 'TRAFFIC OFFENSES', 'HARRASSMENT 2', '(null)',
                'OTHER OFFENSES RELATED TO THEF', 'MISC OFFENSES', 'KIDNAPPING & RELATED OFFENSES', 
                'LOITERING/GAMBLING (CARDS, DIC', 'CRIMINAL MISCHIEF & RELATED OFFENSES', 'HARASSMENT',
                'UNAUTHORIZED USE OF A VEHICLE 3 (UUV)'
            ]
        if 'all' in selected_ethnicities:
            selected_ethnicities = [
                'BLACK', 'WHITE', 'WHITE HISPANIC', 'BLACK HISPANIC',
                'ASIAN / PACIFIC ISLANDER', 'UNKNOWN', 'OTHER',
                'AMERICAN INDIAN/ALASKAN NATIVE'
            ]
        if 'all' in selected_genders:
            selected_genders = ['M', 'F', 'U']
        if 'all' in selected_age_categories:
            selected_age_categories = ['25-44', '45-64', '18-24', '<18', '65+']

        selected_borough_codes = [borough_mapping[borough] for borough in selected_boroughs]

        params = {
            "years": selected_years,
            "boroughs": selected_borough_codes,
            "offenses": selected_offenses,
            "ethnicities": selected_ethnicities,
            "genders": selected_genders,
            "age_categories": selected_age_categories
        }

        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
            future_data = executor.submit(fetch_data, params)
            data = future_data.result()

        data_aggregated = aggregate_data(data, selected_years)

        borough_colors = {
            'MANHATTAN': 'blue',
            'BRONX': 'red',
            'BROOKLYN': 'green',
            'QUEENS': 'purple',
            'STATEN ISLAND': 'orange'
        }

        # Total Crime Count by Borough (bar graph)
        if 'ARREST_BORO' in data_aggregated.columns and 'crime_count' in data_aggregated.columns:
            total_counts = data_aggregated.groupby('ARREST_BORO')['crime_count'].sum().reset_index()
            bar_fig = go.Figure()
            for borough in selected_boroughs:
                borough_code = borough_mapping[borough]
                borough_data = total_counts[total_counts['ARREST_BORO'] == borough_code]
                bar_fig.add_trace(go.Bar(
                    x=[borough], 
                    y=borough_data['crime_count'], 
                    name=borough,
                    marker_color=borough_colors.get(borough, 'grey')
                ))
            bar_fig.update_layout(
                title="Total Crime Count by Borough",
                xaxis_title="Borough",
                yaxis_title="Total Crime Count",
                xaxis=dict(type='category'),
                yaxis=dict(type='linear'),
                legend_title="Borough"
            )
        else:
            bar_fig = go.Figure()

        # Line Plot
        line_fig = go.Figure()
        for borough in selected_boroughs:
            borough_code = borough_mapping[borough]
            borough_data = data_aggregated[data_aggregated['ARREST_BORO'] == borough_code]
            line_fig.add_trace(go.Scatter(x=borough_data['ARREST_DATE'], y=borough_data['crime_count'], mode='lines', name=borough))
        line_fig.update_layout(
            title="Monthly Crime Trends",
            xaxis_title="Date",
            yaxis_title="Crime Count",
            legend_title="Borough"
        )

        # Map Plot
        if 'Latitude' in data.columns and 'Longitude' in data.columns and 'ARREST_BORO' in data.columns:
            map_data = data[['Latitude', 'Longitude', 'ARREST_BORO']].dropna()
            map_fig = px.scatter_mapbox(
                map_data, 
                lat='Latitude', 
                lon='Longitude', 
                color='ARREST_BORO', 
                color_continuous_scale=['yellow', 'grey', 'red'],
                opacity=0.6,
                mapbox_style="open-street-map",
                title="Crime Distribution by Borough"
            )
        else:
            map_fig = go.Figure()

        return bar_fig, line_fig, map_fig

    return {}, {}, {}

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port = 8050)

