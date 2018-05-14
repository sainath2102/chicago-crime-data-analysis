# chicago-crime-data-analysis
Analysis of chicago crime data using core APIs in Spark.
The data for analysis is obtained from this [link](https://data.cityofchicago.org/Public-Safety/Crimes-2017/d62x-nvdr). 

Using Core APIs in Spark some useful insights were drawn from the dataset like the most common types of crime, common locations for different types of crimes, most unsafe times of a day in locations that had highest crime rates.

This dataset reflects reported incidents of crime (with the exception of murders where data exists for each victim) that occurred in the City of Chicago from January 1st 2001 to December 31st 2017 and contains 6595264 records.

The program takes three arguments.
- The first one is to set spark to run in local mode or to connect to a yarn client. It takes one of the two values (local, yarn-client).
- The second argument is the path for input data.
- Finally, the third argument is the path to directory in which the results are stored.

#### To run this application using spark-submit, use the following command:
```
spark-submit --class CrimeData chicago_crime_data/target/scala-2.10/chicago_crime_data_2.10-1.0.jar yarn-client crime_data_txt/chicago_crime.csv crime_data_txt/results
```

## Summary of results obtained are:
- The most common type of crime is 'theft' followed by 'battery' and 'criminal damage' with each constituting to 20, 18 and 11 percent of all the crimes commited respectively. [output link](https://github.com/sainath2102/chicago-crime-data-analysis/blob/master/results/crime_type/part-00000)
- The most common place where the crimes happen is on the streets. Theft (22.2%), criminal damage (15.9%) and narcotics (14.63%) are the most common crimes on streets. [output link 1](https://github.com/sainath2102/chicago-crime-data-analysis/blob/master/results/location_and_crime_type/part-00000) [output link 2](https://github.com/sainath2102/chicago-crime-data-analysis/blob/master/results/location_type/part-00000)
- The number of crimes on the streets between 6PM and 1AM contributes to about 11.75% of all the crimes from 2001 to present in chicago. [output link](https://github.com/sainath2102/chicago-crime-data-analysis/blob/master/results/time_and_location/part-00000)
