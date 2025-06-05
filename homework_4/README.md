Question 2: 

Code should import necessary packages: DAG, PythonOperator, datetime, timedelta, pendulum, time, requests, pandas, boto3, and StringIO. Code then sets up the base url, s3 bucket name, s3 directory, and defines the weather stations of interest as KORD, KENW, KMDW, and KPNT. 

Code then defines the default args. Code then creates the function to collect weather data by ensuring data is from today, iterates through the weather stations, and then should store the details on timeOfCollection, timestamp, station, temperature, dewpoint, windSpeed, barometricPressure, visibility, precipitationLastHour, relativeHumidity, heatIndex into a dataframe. This data is then saved as a csv file in our S3 bucket under directory weather_data.

Code then defines the DAG as collecting the weather data from the selected stations every 2 hours.

Question 3:

Code should import necessary packages: DAG, PythonOperator, datetime, timedelta, pendulum, pandas, boto3, io, and matplotlib.pyplot. Code then sets up the s3 bucket name, input directory as the previously made weather_data directory, and output directory as output_graphs.

Code then defines the default args. Code then creates the function to plot temperature, visibility, and relative humidity by reading data from the csv files in the weather_data directory. It merges and cleans the data, dropping any rows that have invalid timestamps. It then generates the time series plots, and then saves and uploads the plots back to s3 in the output_graphs directory.

Code then defines the DAG as generating daily plots of weather data from S3.

Question 4:

Code should import necessary packages: DAG, PythonOperator, datetime, timedelta, pendulum, pandas, boto3, LinearRegression, matplotlib.pyplot, io, and numpy. Code then defines the input directory as weather_data, and the output directory as predictions.

Code then defines the default args. Code defines task 1 function as combine_all_data() which downloads and combines the daily data from the S3 directory weather_data. It reads files that are from today, combines the csv’s, and saves it to a temporary local file in the folder preprocessed.

Code defines task 2 function, lin_reg(), to perform linear regression. This task performs a time series temperature prediction using a linear regression model. The preprocessed data is loaded and sorted to ensure it is in datetime format and in chronological order. Rows where temperature is missing are dropped, and a sliding window is created for data training. Then a linear regression model is trained to map previous windowed data to the next temperature. The future 16 steps are predicted, and these predictions are saved to the s3 output directory predictions.

Code then defines the DAG as generating predictions of weather data from s3.

