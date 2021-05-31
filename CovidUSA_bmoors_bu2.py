#!/usr/bin/env python
# coding: utf-8

# # United States Covid Dashboards
# 
# Update on refresh is from https://dash.plotly.com/live-updates

# ### Import Libraries

# In[1]:


import pandas as pd
import pymysql
import plotly
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import plotly.express as px
import getpass
import plotly.graph_objects as go
from datetime import datetime
from dash.dependencies import Input, Output
import snowflake.connector
import os
from os import environ

a = environ.get('a')
p = environ.get('p')
u = environ.get('u')

# ### Get DB User

# In[2]:


#try:
#    u = getpass.getpass(prompt='User: ')
#except Exception as error:
#    print('ERROR', error)
#else:
#    print('User entered')


# ### Get DB Password

# In[3]:


#try:
#    p = getpass.getpass()
#except Exception as error:
#    print('ERROR', error)
#else:
#    print('Password entered')
    


# ### Snowflake Connection Test

# In[4]:


# Gets the version
ctx = snowflake.connector.connect(
    user=u,
    password=p,
    account=a
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
    ctx.close()


# ### Establish DB Connection and Pull the Data

# In[5]:


ctx = snowflake.connector.connect(
    user=u,
    password=p,
    account=a,
    warehouse='COMPUTE_WH',
    database='STARSCHEMA_AWS_US_EAST_2_COVID19_BY_STARSCHEMA_DM',
    schema='PUBLIC'
    )
cs = ctx.cursor()

def data_loading():
    pd.options.display.float_format = '{:,}'.format
    results = cs.execute("""
    SELECT DISTINCT
    COUNTRY_REGION,
    CASE_TYPE,
    SUM(CASES),

    DATE
    FROM JHU_COVID_19_TIMESERIES
    WHERE COUNTRY_REGION = 'United States'
    AND CASE_TYPE <> 'Recovered'
    --AND LAST_REPORTED_FLAG = 'TRUE'

    GROUP BY COUNTRY_REGION, CASE_TYPE, DATE

    ORDER BY DATE DESC;""")
    usa_df=pd.DataFrame(results, columns=['COUNTRY_REGION', 'CASE_TYPE', 'CASES', 'DATE'])

    results = cs.execute("""
    SELECT DISTINCT
    COUNTRY_REGION,
    PROVINCE_STATE,
    CONFIRMED,
    DEATHS,
    DATE,
    ISO3166_1,
    ISO3166_2
    FROM JHU_DASHBOARD_COVID_19_GLOBAL
    WHERE COUNTRY_REGION = 'United States'
    --AND PROVINCE_STATE = 'Alabama'
    AND LAST_REPORTED_FLAG = 'TRUE'

    ORDER BY COUNTRY_REGION, PROVINCE_STATE, DATE
    ;""")
    states_df=pd.DataFrame(results, columns=['COUNTRY_REGION', 'PROVINCE_STATE', 'CASES', 'DEATHS', 'DATE', 
                                           'COUNTRY_CODE', 'STATE_CODE'])


    #data wrangling
    usa_cases_df = usa_df[usa_df['CASE_TYPE']=='Confirmed']
    usa_deaths_df = usa_df[usa_df['CASE_TYPE']=='Deaths']
    usa_deaths_df = usa_deaths_df.rename(columns={"CASES": "DEATHS"})
    usa_df = usa_cases_df.join(usa_deaths_df.set_index('DATE'), lsuffix='_caller', rsuffix='_other', on='DATE')
    usa_df = usa_df.drop(['COUNTRY_REGION_other', 'CASE_TYPE_caller', 'CASE_TYPE_other'], axis=1)
    usa_df = usa_df.rename(columns={"COUNTRY_REGION_caller": "COUNTRY_REGION"})
    
    states_df["CASES"] = states_df["CASES"].astype(float)
    states_df["DEATHS"] = states_df["DEATHS"].astype(float)
    
    return usa_df, states_df


# In[7]:


usa_df, states_df = data_loading()


TotalCases = usa_df.head(n=1).CASES[0]
TotalCases


# In[23]:


TotalDeaths = usa_df.head(n=1).DEATHS[0]
TotalDeaths


# ### Build the Dashboard

# In[32]:


def figures(TotalCases, TotalDeaths, usa_df, states_df):
    
    #Total Cases
    fig = go.Figure()
    
    fig.add_trace(go.Indicator(
        value = TotalCases,
        delta = {'reference': 20000000},
        gauge = {'axis': {'range': [None, 20000000]}},
        domain = {'row': 0, 'column': 0}))

    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        grid = {'rows': 1, 'columns': 1, 'pattern': "independent"},
        template = {'data' : {'indicator': [{
            'title': {'text': "Total Cases"},
            'mode' : "number",
            'delta' : {'reference': 90}}]
                         }})
    
    #Total Deaths
    fig2 = go.Figure()
    
    fig2.add_trace(go.Indicator(
        value = TotalDeaths,
        delta = {'reference': 1000000},
        gauge = {'axis': {'range': [None, 1000000]}},
        domain = {'row': 0, 'column': 0}))
    
    fig2.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        grid = {'rows': 1, 'columns': 1, 'pattern': "independent"},
        template = {'data' : {'indicator': [{
            'title': {'text': "Total Deaths"},
            'mode' : "number",
            'delta' : {'reference': 90}}]
                             }})
    
    #Bar Chart
    fig3 = px.bar(usa_df, x="DATE", y="CASES", color="DEATHS", barmode="group")
    
    fig3.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        ),

        legend_title_text='',
        xaxis_title= '',
        yaxis_title= 'Cases'
    )
    
    fig3.update_traces(
        
    hovertemplate='%{x} <br>Cases: %{y:,.0f} <br>Deaths: %{marker.color:,.0f}')
    
    #Time
    fig4 = html.Div([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ], style = {'font-size': '1.0vw', 'background-color':'rgba(0,0,0,0)', "text-align": "center"})
    
    #Choropleth
    fig5 = go.Figure(data=go.Choropleth(
            locations=states_df['STATE_CODE'], # Spatial coordinates
            z = states_df['CASES'], # Data to be color-coded
            locationmode = 'USA-states', # set of locations match entries in `locations`
            text=states_df['PROVINCE_STATE'] + "<br>" + states_df['CASES'].map('{:,.0f}'.format) + ' ' 
            + 'Cases' + "<br>" + states_df['DEATHS'].map('{:,.0f}'.format) + ' ' + 'Deaths',
            hoverinfo = 'text',
            colorscale = 'Purples',
            colorbar_title = 'Cases',
        ))

    fig5.update_layout(
        geo=dict(bgcolor= 'rgba(0,0,0,0)'),
        paper_bgcolor='rgba(0,0,0,0)',
        #title_text = 'USA Covid-19',
        geo_scope='usa' # limite map scope to USA
        #legend=dict(
        #    yanchor="top",
        #    y=0.99,
        #    xanchor="right",
        #    x=0.99
        #),
        #legend_title_text=''
    )
    return fig, fig2, fig3, fig4, fig5


# In[33]:


fig, fig2, fig3, fig4, fig5 = figures(TotalCases, TotalDeaths, usa_df, states_df)
#config = dict({'responsive': True})


# In[34]:


#preview figs if needed
#fig


# In[35]:


#fig2


# In[36]:


#fig3


# In[37]:


#fig4


# In[38]:


#fig5


# In[39]:


def serve_layout():
    usa_df, states_df = data_loading()
    TotalCases = usa_df.head(n=1).CASES[0]
    TotalDeaths = usa_df.head(n=1).DEATHS[0]

    fig, fig2, fig3, fig4, fig5 = figures(TotalCases, TotalDeaths, usa_df, states_df)
    
    #for clarity regarding intervals
    second = 1 * 1000
    minute = 1 * 1000 * 60
    hour = 1 * 1000 * 60 * 60
    
    return dbc.Container([
    #All elements from the top of the page    
    #Set update interval
    dcc.Interval(interval= 5 * minute, id="interval-component"),
    dbc.Row([
        dbc.Col([
            html.Div(style={"height": "15%"}),
                dcc.Graph(
                    style={"height": "75%"},
                    config = dict({'responsive': True}),
                    id='live-update-totalcases',
                    figure=fig
                )
        ], style={"height": "100%", 
                  #"width": "30.6666666667%"
                  "width": "20%"
                 }),
        dbc.Col([
            html.H1(children='USA Covid-19 Tracker'),
                html.Div(fig4, id='live-update-time'),#id to update on interval
                dcc.Graph(
                    style={"height": "100%"},
                    config = dict({'responsive': True}),
                    id='live-update-map',
                    figure=fig5
                )
                #html.Div([
                #    html.Img(src=app.get_asset_url('united-states-png-8053.png'), 
                #        style={"height": "100%",
                #              }
                #    )
                #], style={"height": "85%",                                
                #            'display': 'flex',
                #            'align-items': 'center',
                #            'justify-content': 'center', 
                #         }
                #)
            ], style={"height": "100%", "width": "50%",#style={"height": "100%", "width": "30.6666666667%",
                     }),
        dbc.Col([
            html.Div(style={"height": "15%"}),
                dcc.Graph(
                    style={"height": "75%"},
                    config = dict({'responsive': True}),
                    id='live-update-totaldeaths',
                    figure=fig2
                )
            ], style={"height": "100%", 
                  #"width": "30.6666666667%"
                   "width": "20%"   
                 })
    ],className="h-50"),
    # New Div for all elements in the new 'row' of the page
    dbc.Row([
        dbc.Col([
            dcc.Graph(
                style={"height": "100%"},
                config = dict({'responsive': True}),
                id='live-update-bar',
                figure=fig3
            )
        ], style={"height": "100%", "width": "100%"})
    ],className="h-50") 
    ], style={"height": "100vh"}, fluid=True)


# ### Create the App and Set the Interval Update Functions

# In[40]:


#create the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

#set interval update functions
#update time
@app.callback(Output('live-update-time', 'children'),
              Input('interval-component', 'n_intervals'))
def update_interval_time(n):
    fig4 = html.Div([
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ], style = {'font-size': '1.0vw', 'background-color':'rgba(0,0,0,0)', "text-align": "center"})
    return fig4

#update total cases
@app.callback(Output('live-update-totalcases', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_interval_totalcases(n):
    usa_df, states_df = data_loading()
    TotalCases = usa_df.head(n=1).CASES[0]
    TotalDeaths = usa_df.head(n=1).DEATHS[0]

    fig, fig2, fig3, fig4, fig5 = figures(TotalCases, TotalDeaths, usa_df, states_df)
    
    return fig

#update total deaths
@app.callback(Output('live-update-totaldeaths', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_interval_totaldeaths(n):
    usa_df, states_df = data_loading()
    TotalCases = usa_df.head(n=1).CASES[0]
    TotalDeaths = usa_df.head(n=1).DEATHS[0]

    fig, fig2, fig3, fig4, fig5 = figures(TotalCases, TotalDeaths, usa_df, states_df)
    
    return fig2

#update bar
@app.callback(Output('live-update-bar', 'figure'),
              Input('interval-component', 'n_intervals'))                              
def update_interval_bar(n):
    usa_df, states_df = data_loading()
    TotalCases = usa_df.head(n=1).CASES[0]
    TotalDeaths = usa_df.head(n=1).DEATHS[0]

    fig, fig2, fig3, fig4, fig5 = figures(TotalCases, TotalDeaths, usa_df, states_df)
    
    return fig3

#update map
@app.callback(Output('live-update-map', 'figure'),
              Input('interval-component', 'n_intervals'))                              
def update_interval_map(n):
    usa_df, states_df = data_loading()
    TotalCases = usa_df.head(n=1).CASES[0]
    TotalDeaths = usa_df.head(n=1).DEATHS[0]

    fig, fig2, fig3, fig4, fig5 = figures(TotalCases, TotalDeaths, usa_df, states_df)
    
    return fig5


# ### Run the App

# In[ ]:


app.layout = serve_layout
server = app.server

if __name__ == '__main__':
    app.run_server(debug=False, port=8050, host='127.0.0.1')


# In[ ]:




