#!/usr/bin/env python
# coding: utf-8

# # United States Covid Dashboards
# 
# Update on refresh is from https://dash.plotly.com/live-updates

# ### Import Libraries

# In[1]:


#import mysql.connector as sql
import pandas as pd
#from sqlalchemy import create_engine
import pymysql
import plotly
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import plotly.express as px
import getpass
import plotly.graph_objects as go
#from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from dash.dependencies import Input, Output
import snowflake.connector
import os
from os import environ
#from config import p, u, a

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


# In[6]:


def data_loading():

    results = cs.execute("""
    SELECT m.COUNTRY_REGION, m.DATE, m.CASES, m.DEATHS, m.DEATHS / m.CASES as CFR
    FROM (SELECT COUNTRY_REGION, DATE, AVG(CASES) AS CASES, AVG(DEATHS) AS DEATHS
      FROM ECDC_GLOBAL
      GROUP BY COUNTRY_REGION, DATE) m
    WHERE m.CASES > 0
    AND COUNTRY_REGION = 'United States'
    ORDER BY m.DATE;""")
    df=pd.DataFrame(results, columns=['COUNTRY_REGION', 'DATE', 'CASES', 'DEATHS', 'CFR'])
    print(df)

    #cs.close()
    #ctx.close()

    return df


# In[7]:


df = data_loading()


# ### Explore the Data

# In[8]:


df.head()


# In[9]:


TotalCases = df.sum(axis = 0, skipna = True).CASES
TotalCases


# In[10]:


TotalDeaths = df.sum(axis = 0, skipna = True).DEATHS
TotalDeaths


# ### Build the Dashboard

# In[11]:


def figures(TotalCases, TotalDeaths, df):
    
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
    fig3 = px.bar(df, x="DATE", y="CASES", color="DEATHS", barmode="group")
    
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
    
    fig4 = html.Div([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ], style = {'font-size': '1.0vw', 'background-color':'rgba(0,0,0,0)', "text-align": "center"})
    
    return fig, fig2, fig3, fig4


# In[12]:


fig, fig2, fig3, fig4 = figures(TotalCases, TotalDeaths, df)
#config = dict({'responsive': True})


# In[13]:


#preview figs if needed
fig


# In[14]:


fig2


# In[15]:


fig3


# In[16]:


fig4


# In[17]:


def serve_layout():
    df = data_loading()
    TotalCases = df.sum(axis = 0, skipna = True).CASES
    TotalDeaths = df.sum(axis = 0, skipna = True).DEATHS

    fig, fig2, fig3, fig4 = figures(TotalCases, TotalDeaths, df)
    
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
                  "width": "30.6666666667%"
                 }),
        dbc.Col([
            html.H1(children='USA Covid Stats'),
                html.Div(fig4, id='live-update-time'),#id to update on interval
                html.Div([
                    html.Img(src=app.get_asset_url('united-states-png-8053.png'), 
                        style={"height": "100%",
                              }
                    )
                ], style={"height": "85%",                                
                            'display': 'flex',
                            'align-items': 'center',
                            'justify-content': 'center', 
                         }
                )
            ], style={"height": "100%", "width": "30.6666666667%",
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
                  "width": "30.6666666667%"
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

# In[18]:


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
    df = data_loading()
    TotalCases = df.sum(axis = 0, skipna = True).CASES
    TotalDeaths = df.sum(axis = 0, skipna = True).DEATHS

    fig, fig2, fig3, fig4 = figures(TotalCases, TotalDeaths, df)
    
    return fig

#update total deaths
@app.callback(Output('live-update-totaldeaths', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_interval_totaldeaths(n):
    df = data_loading()
    TotalCases = df.sum(axis = 0, skipna = True).CASES
    TotalDeaths = df.sum(axis = 0, skipna = True).DEATHS

    fig, fig2, fig3, fig4 = figures(TotalCases, TotalDeaths, df)
    
    return fig2

#update bar
@app.callback(Output('live-update-bar', 'figure'),
              Input('interval-component', 'n_intervals'))                              
def update_interval_bar(n):
    df = data_loading()
    TotalCases = df.sum(axis = 0, skipna = True).CASES
    TotalDeaths = df.sum(axis = 0, skipna = True).DEATHS

    fig, fig2, fig3, fig4 = figures(TotalCases, TotalDeaths, df)
    
    return fig3


# ### Run the App

# In[ ]:


app.layout = serve_layout
server = app.server
if __name__ == '__main__':
    app.run_server(debug=False, port=8050)


# In[ ]:




