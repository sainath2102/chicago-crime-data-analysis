/**
  * Created by sainath on 05/11/2018.
  */;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object CrimeData {
  def main(args: Array[String])=
  {
    /* The main function takes three arguments.
        The first argument is used to select the mode in which we run spark. It can be yarn-client/local depending on the setup used to run the program.
        The second is the path for the input data file. The input data is in text file format.
        The third argument is the output directory. All the results are stored in the given directory.
     */
    val conf = new SparkConf().setAppName("Chicago crime data analysis").setMaster(args(0));
    val sc = new SparkContext(conf);
    val input_data = sc.textFile(args(1));

    val header = input_data.first()
    val crime_data_wo_header = input_data.filter(x => x != header)

    // We have selected to include only the ID, Date, Primary Type, Location Description, Year features from the given data for our analysis.
    //(ID, Date, Primary Type, Location Description, Year).
    val crime_data= crime_data_wo_header.map(x =>{
      val i = x.split(",")
      (i(0),i(2),i(5),i(7),i(17))
    })

    // we get the total number of records(crimes).
    val total = crime_data.count()

    // (Primary Type, Count, Percentage = (count/total)*100)
    val count_by_type = crime_data.
                        map(x => (x._3,1)).
                        reduceByKey(_+_).
                        sortBy(x=>(-x._2)).
                        map(x => (x._1, x._2 ,x._2*100/total))

    // This gives the most common types of crime that takes place in chicago.
    count_by_type.
      map(x=> x._1+","+x._2+","+x._3).
      coalesce(4).
      saveAsTextFile(args(2)+"/crime_type");

    //(Location Description, Count)
    val count_by_location = crime_data.
                            map(x => (x._4,1)).
                            reduceByKey(_+_).
                            sortBy(x=>(-x._2))
    //This gives the most common types of places for crimes.
    count_by_location.
      map(x=> x._1+","+x._2).
      coalesce(4).
      saveAsTextFile(args(2)+"/location_type");


    //(Location, Type, count of type of crime, count of total crimes in the Location type, percentage of this type of crimes in this category of location)
    val count_by_type_and_location = crime_data.
                                      map(x => ((x._4,x._3),1)).
                                      reduceByKey(_+_).
                                      map(x => ((x._1._1),x)).
                                      join(count_by_location).
                                      map(x => (x._2._1._1._1, x._2._1._1._2, x._2._1._2, x._2._2, x._2._1._2.toFloat*100/x._2._2)).
                                      sortBy(x => (-x._4, -x._5))
    // This gives the percentage of different types of crime in the location.
    count_by_type_and_location.
      map(x=> x._1+","+x._2+","+x._3+","+x._4+","+x._5).
      coalesce(4).
      saveAsTextFile(args(2)+"/location_and_crime_type");

    //(time, location, count)
    val count_by_time_and_location = crime_data.
                                      map(x => ((x._2.split(" ")(1).split(":")(0)+x._2.split(" ")(2),x._4),1)).
                                      reduceByKey(_+_).
                                      sortBy(x => -x._2).
                                      map(x=> (x._1._1,x._1._2,x._2))

    //This gives the most risky times to be in a particular location type.
    count_by_time_and_location.
      map(x=> x._1+","+x._2+","+x._3).
      coalesce(4).
      saveAsTextFile(args(2)+"/time_and_location")

  }
}
