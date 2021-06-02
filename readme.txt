Test Environment:
- Windows 10 Pro OS
- Anaconda CLI
- Jupyter Notebooks
- Dataset is located on Snowflake account and is a public dataset located here:
	https://www.snowflake.com/datasets/starschema-covid-19-epidemiological-data/

Prod Environment:
- Same dataset as test, located on Snowflake account.  Dataset is public and can be found here:
	https://www.snowflake.com/datasets/starschema-covid-19-epidemiological-data/
- AWS EC2 with linux OS
- Dashboard built using python plotly and dashboard
- Dashboard hosted using gunicorn and heroku
- Deployed to heroku using instructions here:
	https://dash.plotly.com/deployment
