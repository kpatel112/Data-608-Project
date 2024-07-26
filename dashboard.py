#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import boto3

# Initialize Dash app
app = dash.Dash(__name__)

# Set up AWS S3 connection
s3 = boto3.client('s3')
bucket_name = '608project'

def load_data(year):
    file_path = f's3://{bucket_name}/data_{year}.parquet'
    return pd.read_parquet(file_path)

# Define the layout of the app
app.layout = html.Div([
    html.H1("New York Crime Data Dashboard"),
    dcc.Dropdown(
        id='year-dropdown',
        options=[{'label': str(year), 'value': year} for year in range(2001, 2024)],
        value=2023,
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='borough-dropdown',
        options=[
            {'label': 'Manhattan', 'value': 'MANHATTAN'},
            {'label': 'Bronx', 'value': 'BRONX'},
            {'label': 'Brooklyn', 'value': 'BROOKLYN'},
            {'label': 'Queens', 'value': 'QUEENS'},
            {'label': 'Staten Island', 'value': 'STATEN ISLAND'}
        ],
        value='MANHATTAN',
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
        ]],
        value='VIOLENT CRIMES',
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='ethnicity-dropdown',
        options=[{'label': ethnicity, 'value': ethnicity} for ethnicity in [
            'BLACK', 'WHITE', 'WHITE HISPANIC', 'BLACK HISPANIC',
            'ASIAN / PACIFIC ISLANDER', 'UNKNOWN', 'OTHER',
            'AMERICAN INDIAN/ALASKAN NATIVE'
        ]],
        value='BLACK',
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='gender-dropdown',
        options=[{'label': gender, 'value': gender} for gender in ['M', 'F', 'U']],
        value='M',
        style={'width': '50%'}
    ),
    dcc.Dropdown(
        id='age-category-dropdown',
        options=[{'label': age, 'value': age} for age in [
            '25-44', '45-64', '18-24', '<18', '65+'
        ]],
        value='25-44',
        style={'width': '50%'}
    ),
    html.Button('Apply', id='apply-button', n_clicks=0),
    dcc.Graph(id='crime-graph'),
    html.Div(id='output-container')
])

# Define the callback to update data based on selected filters
@app.callback(
    Output('crime-graph', 'figure'),
    [Input('apply-button', 'n_clicks')],
    [
        dash.dependencies.State('year-dropdown', 'value'),
        dash.dependencies.State('borough-dropdown', 'value'),
        dash.dependencies.State('offense-dropdown', 'value'),
        dash.dependencies.State('ethnicity-dropdown', 'value'),
        dash.dependencies.State('gender-dropdown', 'value'),
        dash.dependencies.State('age-category-dropdown', 'value')
    ]
)
def update_output(n_clicks, selected_year, selected_borough, selected_offense, selected_ethnicity, selected_gender, selected_age_category):
    if n_clicks > 0:
        data = load_data(selected_year)
        filtered_data = data[
            (data['borough'] == selected_borough) &
            (data['offense'] == selected_offense) &
            (data['ethnicity'] == selected_ethnicity) &
            (data['gender'] == selected_gender) &
            (data['age_category'] == selected_age_category)
        ]
        
        # Example visualization
        fig = px.scatter(filtered_data, x='date', y='crime_count', color='crime_type', 
                         title=f'Crime Data for {selected_borough} in {selected_year}')
        
        return fig
    return px.scatter(title="Select filters and click Apply")

# Run the application
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')

