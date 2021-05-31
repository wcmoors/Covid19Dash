To Run App:

- open Anaconda
- pip install python and all libraries in CovidUSA_bmoors.py
- pip install gunicorn (for unix machines)
- install waitress (for windows machines)

To Run App on Windows

- Change directory to directory with CovidUSA_bmoors in it
- Run "waitress-serve --listen=<serverip>:8050 CovidUSA_bmoors:app.server"

To Run App on Ubuntu:

- ssh into ubuntu server
- "gunicorn CovidUSA_bmoors:server -b <serverip>:8050 -D" (-D allows you to exit the putty session without stopping the dashboard)

To Stop App on Ubuntu:

- "ps ax|grep gunicorn"
- "kill -9 <pid number>"
