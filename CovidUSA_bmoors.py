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
w = environ.get('w')
d = environ.get('d')
s = environ.get('s')

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
    warehouse=w,
    database=d,
    schema=s
    )
cs = ctx.cursor()

def data_loading():
    pd.options.display.float_format = '{:,}'.format
    results = cs.execute("""
    SELECT
    covid.COUNTRY_REGION,
    covid.CASE_TYPE,
    covid.CASES,
    covid.DIFFERENCE,
    covid.DATE,
    pop.POPULATION,
    IFNULL(vax.TOTAL_VACCINATIONS, 0) AS TOTAL_VACCINATIONS,
    IFNULL(vax.PEOPLE_FULLY_VACCINATED, 0) AS PEOPLE_FULLY_VACCINATED,
    IFNULL(vax.DAILY_VACCINATIONS, 0) AS DAILY_VACCINATIONS
    
    FROM (
    SELECT DISTINCT
    ISO3166_1,
    COUNTRY_REGION,
    CASE_TYPE,
    SUM(CASES) AS CASES,
    SUM(DIFFERENCE) AS DIFFERENCE,
    DATE
    
    FROM JHU_COVID_19_TIMESERIES
    
    WHERE COUNTRY_REGION = 'United States'
    AND CASE_TYPE <> 'Recovered'
    --AND LAST_REPORTED_FLAG = 'TRUE'

    GROUP BY COUNTRY_REGION, ISO3166_1, CASE_TYPE, DATE
    ) covid
    
    INNER JOIN (
    SELECT
    ISO3166_1,
    SUM(TOTAL_POPULATION) AS POPULATION
    FROM DEMOGRAPHICS
    GROUP BY ISO3166_1
    ) pop ON pop.ISO3166_1 = covid.ISO3166_1
    
    LEFT OUTER JOIN (
    SELECT
    DATE,
    COUNTRY_REGION,
    TOTAL_VACCINATIONS,
    PEOPLE_FULLY_VACCINATED,
    DAILY_VACCINATIONS
    FROM OWID_VACCINATIONS
    WHERE COUNTRY_REGION = 'United States'
    ) vax ON vax.COUNTRY_REGION = covid.COUNTRY_REGION
        AND vax.DATE = covid.DATE

    ORDER BY covid.DATE DESC;""")
    usa_df=pd.DataFrame(results, columns=['COUNTRY_REGION', 'CASE_TYPE', 'CASES', 'DIFFERENCE','DATE', 'POPULATION',
                                          'TOTAL_VACCINATIONS','PEOPLE_FULLY_VACCINATED','DAILY_VACCINATIONS'])
    
    #FOR LINE CHART
    #results = cs.execute("""
    #SELECT DISTINCT
    #COUNTRY_REGION,
    #CASE_TYPE,
    #SUM(CASES),
    #SUM(DIFFERENCE),
    #DATE
    #
    #FROM JHU_COVID_19_TIMESERIES
    #WHERE COUNTRY_REGION = 'United States'
    #AND CASE_TYPE <> 'Recovered'
    #--AND LAST_REPORTED_FLAG = 'TRUE'
    #
    #GROUP BY COUNTRY_REGION, CASE_TYPE, DATE
    #
    #ORDER BY DATE DESC
    #""")
    #usats_df=pd.DataFrame(results, columns=['COUNTRY_REGION', 'CASE_TYPE', 'CASES', 'DIFFERENCE','DATE'])
    
    results = cs.execute("""
    SELECT DISTINCT
    JHU_DASHBOARD_COVID_19_GLOBAL.COUNTRY_REGION,
    JHU_DASHBOARD_COVID_19_GLOBAL.PROVINCE_STATE,
    JHU_DASHBOARD_COVID_19_GLOBAL.CONFIRMED,
    JHU_DASHBOARD_COVID_19_GLOBAL.DEATHS,
    JHU_DASHBOARD_COVID_19_GLOBAL.DATE,
    JHU_DASHBOARD_COVID_19_GLOBAL.ISO3166_1,
    JHU_DASHBOARD_COVID_19_GLOBAL.ISO3166_2,
    vax.DOSES_ADMIN_TOTAL,
    IFNULL(vax.PEOPLE_TOTAL_2ND_DOSE,0) AS FULLY_VACCINATED,
    pop.POPULATION
    FROM JHU_DASHBOARD_COVID_19_GLOBAL
    INNER JOIN (
        SELECT
        STABBR,
        MAX(DOSES_ADMIN_TOTAL) AS DOSES_ADMIN_TOTAL,
        MAX(PEOPLE_TOTAL_2ND_DOSE) AS PEOPLE_TOTAL_2ND_DOSE
        FROM JHU_VACCINES 
        GROUP BY STABBR
    ) vax ON vax.STABBR = JHU_DASHBOARD_COVID_19_GLOBAL.ISO3166_2
    INNER JOIN ( --inner join excludes anything not in the 50 united states
        SELECT
        STATE,
        SUM(TOTAL_POPULATION) AS POPULATION
        FROM DEMOGRAPHICS
        GROUP BY STATE
    ) pop on pop.STATE = JHU_DASHBOARD_COVID_19_GLOBAL.ISO3166_2
    WHERE JHU_DASHBOARD_COVID_19_GLOBAL.COUNTRY_REGION = 'United States'
    AND JHU_DASHBOARD_COVID_19_GLOBAL.PROVINCE_STATE <> 'Diamond Princess'
    AND JHU_DASHBOARD_COVID_19_GLOBAL.PROVINCE_STATE <> 'Grand Princess'
    --AND PROVINCE_STATE = 'Alabama'
    AND JHU_DASHBOARD_COVID_19_GLOBAL.LAST_REPORTED_FLAG = 'TRUE'

    ORDER BY COUNTRY_REGION, PROVINCE_STATE, DATE
    """)
    states_df=pd.DataFrame(results, columns=['COUNTRY_REGION', 'PROVINCE_STATE', 'CASES', 'DEATHS', 'DATE', 
                                           'COUNTRY_CODE', 'STATE_CODE', 'DOSES_ADMIN_TOTAL',
                                            'FULLY_VACCINATED', 'POPULATION'])


    #data wrangling
    usa_cases_df = usa_df[usa_df['CASE_TYPE']=='Confirmed']
    usa_deaths_df = usa_df[usa_df['CASE_TYPE']=='Deaths']
    usa_deaths_df = usa_deaths_df.rename(columns={"CASES": "DEATHS"})
    usa_df = usa_cases_df.join(usa_deaths_df.set_index('DATE'), lsuffix='_caller', rsuffix='_other', on='DATE')
    usa_df = usa_df.drop(['COUNTRY_REGION_other', 'CASE_TYPE_caller', 'CASE_TYPE_other', 'POPULATION_caller',
                         'TOTAL_VACCINATIONS_caller','PEOPLE_FULLY_VACCINATED_caller','DAILY_VACCINATIONS_caller'], axis=1)
    usa_df = usa_df.rename(columns={"COUNTRY_REGION_caller": "COUNTRY_REGION", "DIFFERENCE_caller": "DIFFERENCE_CASES",
                                   "DIFFERENCE_other": "DIFFERENCE_DEATHS", "POPULATION_other": "POPULATION",
                                   'TOTAL_VACCINATIONS_other':'TOTAL_VACCINATIONS',
                                    'PEOPLE_FULLY_VACCINATED_other':'PEOPLE_FULLY_VACCINATED',
                                    'DAILY_VACCINATIONS_other':'DAILY_VACCINATIONS'})
    usa_df["POPULATION"] = usa_df["POPULATION"].astype(float)
    states_df["CASES"] = states_df["CASES"].astype(float)
    states_df["DEATHS"] = states_df["DEATHS"].astype(float)
    
    return usa_df, states_df


# In[7]:


usa_df, states_df = data_loading()


TotalCases = usa_df['CASES'].iloc[0]
TotalDeaths = usa_df['DEATHS'].iloc[0]
TotalPop = usa_df['POPULATION'].iloc[0]
TotalVax = usa_df['PEOPLE_FULLY_VACCINATED'].iloc[0]
PercentVax = round(TotalVax / TotalPop * 100)

# ### Build the Dashboard

# In[32]:


def figures(TotalCases, TotalDeaths, PercentVax, TotalPop, usa_df, states_df):
    
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
    
    #Percentage Vaccinated
    fig2 = go.Figure()
    
    fig2.add_trace(go.Indicator(
        value = PercentVax,
        number = {'suffix':'%'},
        delta = {'reference': TotalPop},
        gauge = {'axis': {'range': [None, TotalPop]},
                'bar': {'color': "rebeccapurple"},},
        domain = {'row': 0, 'column': 0}))
    
    fig2.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        grid = {'rows': 1, 'columns': 1, 'pattern': "independent"},
        template = {'data' : {'indicator': [{
            'title': {'text': "Fully Vaccinated"},
            'mode' : "number",
            'delta' : {'reference': 90}}]
                             }})
    
    #Bar Chart
    fig3 = px.bar(usa_df, x="DATE", y="DIFFERENCE_CASES", color="DAILY_VACCINATIONS", barmode="group")
    
    fig3.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        ),
        #coloraxis.colorbar.title = 'Deaths',
        xaxis_title= '',
        yaxis_title= 'Cases'
    )
    fig3.layout.coloraxis.colorbar.title = 'Vaccinations'
    fig3.update_traces(
        hovertemplate='%{x} <br>Cases: %{y:,.0f} <br>Vaccinations: %{marker.color:,.0f}' 
            + '<br>' + 'Fully Vaccinated: %{text}',
                text=#states_df['PROVINCE_STATE'] 
                #+ "<br>" + states_df['POPULATION'].map('{:,.0f}'.format) + ' ' + 'Population' 
                #+ "<br>"
                
                (usa_df['PEOPLE_FULLY_VACCINATED']/usa_df['POPULATION']).map('{:,.0%}'.format)
   
                
            #hoverinfo = 'text'
    )
    #fig3.add_trace(go.Scatter(
    #    mode='lines+markers',
    #    x = usa_df['DATE'],
    #    y = usa_df['DAILY_VACCINATIONS'],
    #    name="Daily Vaccinations",
    #    marker_color='crimson'
    #))
    fig3.add_trace(
    go.Scatter(
        mode='markers',
        x=['2020-12-21'],
        y=[300000],
        marker=dict(
            color='papayawhip',
            size=15,
            line=dict(
                color='black',
                width=1
            ),
            symbol='asterisk'
        ),

        showlegend=False,
        text='Vaccinations started Dec 21, 2020',
        hoverinfo = 'text',
    )
    )
    
    #Time
    fig4 = html.Div([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ], style = {'font-size': '1.0vw', 'background-color':'rgba(0,0,0,0)', "text-align": "center"})
    
    #Choropleth
    fig5 = go.Figure(data=go.Choropleth(
            locations=states_df['STATE_CODE'], # Spatial coordinates
            z = (states_df['FULLY_VACCINATED']/states_df['POPULATION'])*100,#states_df['CASES'], # Data to be color-coded
            locationmode = 'USA-states', # set of locations match entries in `locations`
            text=states_df['PROVINCE_STATE'] 
                #+ "<br>" + states_df['POPULATION'].map('{:,.0f}'.format) + ' ' + 'Population' 
                #+ "<br>"
                + "<br>" + (states_df['FULLY_VACCINATED']/states_df['POPULATION']).map('{:,.0%}'.format) + ' ' + 'Fully Vaccinated'
                + "<br>"
                #+ "<br>" + states_df['CASES'].map('{:,.0f}'.format) + ' ' + 'Cases' 
                + "<br>" + (states_df['CASES']/states_df['POPULATION']).map('{:,.0%}'.format) + ' ' + 'Cases/Pop' 
                #+ "<br>"
                #+ "<br>" + states_df['DEATHS'].map('{:,.0f}'.format) + ' ' + 'Deaths'
                + "<br>" + (states_df['DEATHS']/states_df['POPULATION']).map('{:,.2%}'.format) + ' ' + 'Deaths/Pop'
                + "<br>"
                + "<br>" + "{:.0%}".format((TotalCases/TotalPop)) + ' ' + 'US Cases/Pop'
                + "<br>" + "{:.2%}".format((TotalDeaths/TotalPop)) + ' ' + 'US Deaths/Pop',    
                
            hoverinfo = 'text',
            colorscale = 'Purples'
            #colorbar_title = 'Cases',

        ))

    fig5.update_layout(
        geo=dict(bgcolor= 'rgba(0,0,0,0)'),
        paper_bgcolor='rgba(0,0,0,0)',
        title_text = '',
        #title={
        #'text': "Percent Fully Vaccinated",
        #'y':0.8,
        #'x':0.5,
        #'xanchor': 'center',
        #'yanchor': 'top'
        #},
        #coloraxis_colorbar_tickformat=':.2%',
        coloraxis_colorbar_title='test',
        coloraxis_colorbar_ticksuffix="%",
        geo_scope='usa', # limite map scope to USA
        #legend=dict(
        #    yanchor="bottom",
        #    y=0.99,
        #    xanchor="bottom",
        #    x=0.99
        #),
        #legend_title_text=''
    )
    #fig5.update_coloraxes(colorbar_tickformat=':.2%')
    #fig5.data[0].colorbar.x=0.85
    fig5.data[0].colorbar.ticksuffix="%"
    fig5.data[0].colorbar.title="% Fully<br>Vaccinated"
    #line chart   
    #fig6 = px.line(usats_df, x="DATE", y="DIFFERENCE", color='CASE_TYPE')

    
    return fig, fig2, fig3, fig4, fig5


# In[33]:


fig, fig2, fig3, fig4, fig5, = figures(TotalCases, TotalDeaths, PercentVax, TotalPop, usa_df, states_df)
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
    TotalCases = usa_df['CASES'].iloc[0]
    TotalDeaths = usa_df['DEATHS'].iloc[0]
    TotalPop = usa_df['POPULATION'].iloc[0]
    TotalVax = usa_df['PEOPLE_FULLY_VACCINATED'].iloc[0]
    PercentVax = round(TotalVax / TotalPop * 100)
    
    fig, fig2, fig3, fig4, fig5, = figures(TotalCases, TotalDeaths, PercentVax, TotalPop, usa_df, states_df)
    
    #for clarity regarding intervals
    second = 1 * 1000
    minute = 1 * 1000 * 60
    hour = 1 * 1000 * 60 * 60
    day = 24 * hour
    
    return dbc.Container([
    #All elements from the top of the page    
    #Set update interval
    dcc.Interval(interval= day, id="interval-component"),
    dbc.Row([
                dbc.Col([
            html.H1(children='USA Covid-19 Tracker'),
                html.Div(fig4, id='live-update-time'),#id to update on interval
                ], style={"height": "100%", "width": "47%",#style={"height": "100%", "width": "30.6666666667%",
                     })
    ],style={"height": "2vh"}),#)
    dbc.Row([
        dbc.Col([
            
                dcc.Graph(
                    style={"height": "100%"},
                    config = dict({'responsive': True}),
                    id='live-update-totalcases',
                    figure=fig
                )
        ], style={"height": "100%", 
                  "width": "30.6666666667%"
                 }),
        dbc.Col([
            #html.H1(children='USA Covid-19 Tracker'),
                #html.Div(fig4, id='live-update-time'),#id to update on interval
                dcc.Graph(
                    style={"height": "100%"},
                    config = dict({'responsive': True}),
                    id='live-update-map',
                    figure=fig5
                ),
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
            ], style={"height": "100%", "width": "47%",#style={"height": "100%", "width": "30.6666666667%",
                     }),
        dbc.Col([
            
                dcc.Graph(
                    style={"height": "100%"},
                    config = dict({'responsive': True}),
                    id='live-update-percentvax',
                    figure=fig2
                )
            ], style={"height": "100%", 
                      "width": "47%" 
                  #"width": "30.6666666667%" 
                 })

    ],style={"height": "50vh"}),#,className="h-50"),
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
    ],style={"height": "48vh"})#className="h-50") 
    ], style={"height": "100vh"}, fluid=True)


# ### Create the App and Set the Interval Update Functions

# In[40]:


#create the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP]#,     
			#meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}]
		)

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
    TotalCases = usa_df['CASES'].iloc[0]
    TotalDeaths = usa_df['DEATHS'].iloc[0]
    TotalPop = usa_df['POPULATION'].iloc[0]
    TotalVax = usa_df['PEOPLE_FULLY_VACCINATED'].iloc[0]
    PercentVax = round(TotalVax / TotalPop * 100)
    
    fig, fig2, fig3, fig4, fig5, = figures(TotalCases, TotalDeaths, PercentVax, TotalPop, usa_df, states_df)
    
    return fig

#update percent fully vaccinated
@app.callback(Output('live-update-percentvax', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_interval_percentvax(n):
    usa_df, states_df = data_loading()
    TotalCases = usa_df['CASES'].iloc[0]
    TotalDeaths = usa_df['DEATHS'].iloc[0]
    TotalPop = usa_df['POPULATION'].iloc[0]
    TotalVax = usa_df['PEOPLE_FULLY_VACCINATED'].iloc[0]
    PercentVax = round(TotalVax / TotalPop * 100)
    
    fig, fig2, fig3, fig4, fig5, = figures(TotalCases, TotalDeaths, PercentVax, TotalPop, usa_df, states_df)
    
    return fig2

#update bar
@app.callback(Output('live-update-bar', 'figure'),
              Input('interval-component', 'n_intervals'))                              
def update_interval_bar(n):
    usa_df, states_df = data_loading()
    TotalCases = usa_df['CASES'].iloc[0]
    TotalDeaths = usa_df['DEATHS'].iloc[0]
    TotalPop = usa_df['POPULATION'].iloc[0]
    TotalVax = usa_df['PEOPLE_FULLY_VACCINATED'].iloc[0]
    PercentVax = round(TotalVax / TotalPop * 100)
    
    fig, fig2, fig3, fig4, fig5, = figures(TotalCases, TotalDeaths, PercentVax, TotalPop, usa_df, states_df)
    
    return fig3

#update map
@app.callback(Output('live-update-map', 'figure'),
              Input('interval-component', 'n_intervals'))                              
def update_interval_map(n):
    usa_df, states_df = data_loading()
    TotalCases = usa_df['CASES'].iloc[0]
    TotalDeaths = usa_df['DEATHS'].iloc[0]
    TotalPop = usa_df['POPULATION'].iloc[0]
    TotalVax = usa_df['PEOPLE_FULLY_VACCINATED'].iloc[0]
    PercentVax = round(TotalVax / TotalPop * 100)
    
    fig, fig2, fig3, fig4, fig5, = figures(TotalCases, TotalDeaths, PercentVax, TotalPop, usa_df, states_df)
    
    return fig5


# ### Run the App

# In[ ]:


app.layout = serve_layout
server = app.server

if __name__ == '__main__':
    app.run_server(debug=False, port=8050, host='127.0.0.1')


# In[ ]:




