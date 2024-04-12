# Worflow-Orchestration-in-Airflow-for-NYC-transportation-data
Utilised the Airflow workflow orchestration platform to extract, transform and load NYC transportation data recorded for 3 years (2018-2020) to the  MySQL database server and generate Monthly Summary Statistics reports.

The Yellow Taxi trip data can be accessed using the following JSON API endpoints:

2018: https://data.cityofnewyork.us/resource/t29m-gskq.json

2019: https://data.cityofnewyork.us/resource/2upf-qytp.json

2020: https://data.cityofnewyork.us/resource/kxp8-n2sj.json

Generated Monthly Summary Statistics reports providing the following information:

1. Trips per day - Average number of trips recorded each day
2. Farebox per day - Total amount, across all vehicles, collected from all fares, surcharges, taxes, and tolls. Note: this amount does not include
   amounts from credit card tips
3. Pickups and dropoffs at each Borough per month - Total number of pickups and dropoffs at all locations in each borough.
4. Trip miles per month over time - Total distance in miles reported by the taximeter per month from 2018 to 2020
