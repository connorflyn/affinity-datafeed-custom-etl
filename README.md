# affinity-datafeed-etl Integration

This integration is used to collect data from the Affinity Datafeed API

## Prerequisites

This program will require the following credentials (*All credentials should be stored in the local repository's `.env` file*);

  1. `AFFINITY_CLIENT_ID`
  2. `AFFINITY_CLIENT_SECRET`
  3. `REDSHIFT_HOST`
  4. `REDSHIFT_USERNAME`
  5. `REDSHIFT_PORT`
  6. `REDSHIFT_DATABASE`
  7. `REDSHIFT_PASSWORD`
  8. `REDSHIFT_SCHEMA`

 ## Running the Brand (list) program

 1. Pull the repository
 2. Add the `.env` file to the local repository
 3. Run the Brands program with the shell command;
```
python3 affinity_companies_etl.py
```

 ## Running the Report program
 
 1. Pull the repository
 2. Add the `.env` file to the local repository


 *This leaves two options run the program* [Dynamic date(s)] *or* [Custom Date Range]
 
 
 ### Dynamic Configurations
 3. Run the program using the command:
  - This will call the API and load in last weeks `datafeed_brands_run_time={insight_run_time}` Report
 
```
python3 affinity_report_etl.py
```
 
 ### Custom Configurations
 3. Run the program using the following command with arguments for:
  - *[optional]* Date Range: Must include both of the following, *or neither* 
    - `--start_date`: MIN date for API call
    - `--end_date`: MAX date for API call
  - *[optional]* Report name:
    - `--insights_name`: The name of the Insight Report you will be creating
  - *[optional]* Reporting Interval:
    - `--breakout_interval`: Report grouping by options of `weekly`, `monthly` or `yearly`

 
```
python3 affinity_report_etl.py --start_date 20230109 --end_date 20220113 --breakout_interval weekly --insights_name Year_Prior__Weekly_Report
```


 ### Manually extract & load an insight report (based off an inishght ID)
 *These stepos assume the report was not available, or ready, when you ran `affinity_report_etl.py`. If this occures you can take the `insight_id` provided from the inital Post request to manually pull down the report and load it into the Data Lake & Redshift*
 
 1. Pull the repository
 2. Run the manual program
 ```
python3 report_manual_pull.py -id 66666
```
