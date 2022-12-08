import os
from itertools import permutations

from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import mean


def restaurant_shift_coworkers(worker_shifts: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the worker_shifts.txt. Performs a series of MapReduce operations via
    PySpark to calculate the number of shifts worked together by each pair of co-workers. Returns the results as an RDD
    sorted by the number of shifts worked together THEN by the names of co-workers in a DESCENDING order.
    :param worker_shifts: RDD object of the contents of worker_shifts.txt.
    :return: RDD of pairs of co-workers and the number of shifts they worked together sorted in a DESCENDING order by
             the number of shifts then by the names of co-workers.
             Example output: [(('Shreya Chmela', 'Fabian Henderson'), 3),
                              (('Fabian Henderson', 'Shreya Chmela'), 3),
                              (('Shreya Chmela', 'Leila Jager'), 2),
                              (('Leila Jager', 'Shreya Chmela'), 2)]
    """
    worker_shift_mapped = worker_shifts.flatMap(
        lambda line:  [(line.split(",")[1], line.split(",")[0])])
    # worker_shift_grouped = worker_shift_mapped.groupByKey().mapValues(list)
    # worker_shift_mapped_grouped = worker_shift_grouped.cartesian(worker_shift_mapped).filter(lambda x: x[1] not in x[0])
    # print("worker_shift_grouped",worker_shift_mapped_grouped.take(1))
    # worker_names = worker_shift_mapped.flatMap(
    #     lambda line: [line[1]]).distinct()
    # worker_names_mapped = worker_names.cartesian(worker_names).filter(lambda x: x[0][1] not in x[1][1]).map(lambda x: [((item),1) for item in permutations(x, 2)]).flatMap(lambda line: line)

    #return type [((str, str),1)]
    worker_shift_mapped = worker_shift_mapped.cartesian(worker_shift_mapped).filter(lambda x: x[0][0] == x[1][0]).filter(lambda x: x[0][1] not in x[1][1]).map(lambda i : ((i[0][1],i[1][1]),1))
    
    #return type [((str, str),int)]
    worker_shift_filtered = worker_shift_mapped.reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1],False)
    # print(worker_shift_mapped.reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1],False).take(4))

    # worker_shift_merged = worker_shift_mapped.join(worker_shift_mapped_reverse).filter(lambda x : x[1][0]!=x[1][1]).map(lambda x : (x[1],1))

    # worker_shift = worker_shift_mapped.flatMap(lambda line: filterData(line,worker_shift_mapped))

    # print("worker_shift_mapped",)
    # print("worker_shift", worker_shift.reduceByKey(lambda x, y: x+","+ y).collect())
    # worker_shift_mapped = worker_shifts.flatMap(lambda x: [(i[-10:], 1) for i in x])

    # raise NotImplementedError('Your Implementation Here.')
    return worker_shift_filtered


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """

    year, month = [2021, 9]
    df_filtered_by_date = flights.filter(
        flights.Year == year).filter(flights.Month == month)

    df_rdd = df_filtered_by_date.select(
        ["Airline"]).rdd.flatMap(lambda row: [(row, 1)])
    return df_rdd.reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], ascending=False).first()[0].__getitem__("Airline")

    # raise NotImplementedError('Your Implementation Here.')


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of 
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """

    year, month, dateRange = 2021, 11, [20, 30]
    df_filtered_by_date = flights.filter((flights.Diverted == True) & (flights.Year == year) & (
        flights.Month == month) & (flights.DayofMonth >= dateRange[0]) & (flights.DayofMonth <= dateRange[1]))
    return df_filtered_by_date.count()

    # raise NotImplementedError('Your Implementation Here.')


def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    """

    OriginCityName, DestCityName = "Nashville", "Chicago"
    df_filtered_by_date = flights.filter(flights.OriginCityName.contains(OriginCityName)).filter(
        flights.DestCityName.contains(DestCityName)).na.drop().select(mean("AirTime")).collect()
    return df_filtered_by_date[0].__getitem__("avg(AirTime)")
    # raise NotImplementedError('Your Implementation Here.')


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is 
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing. 
    """

    df_filtered_by_date = flights.filter(flights.DepTime.isNull(
    )).dropDuplicates().select("FlightDate").dropDuplicates()
    return df_filtered_by_date.count()

    # raise NotImplementedError('Your Implementation Here.')


def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext('local[*]')
    spark = SparkSession.builder.getOrCreate()

    print('########################## Problem 1 ########################')
    # problem 1: restaurant shift coworkers with Spark and MapReduce
    # read the file
    worker_shifts = sc.textFile('worker_shifts.txt')
    sorted_num_coworking_shifts = restaurant_shift_coworkers(worker_shifts)
    # print the most, least, and average number of shifts together
    sorted_num_coworking_shifts.persist()
    print('Co-Workers with most shifts together:', sorted_num_coworking_shifts.first())
    print('Co-Workers with least shifts together:', sorted_num_coworking_shifts.sortBy(lambda x: (x[1], x[0])).first())
    print('Avg. No. of Shared Shifts:',
          sorted_num_coworking_shifts.map(lambda x: x[1]).reduce(lambda x,y: x+y)/sorted_num_coworking_shifts.count())

    print('########################## Problem 2 ########################')
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv('Combined_Flights_2021.csv',
                             header=True, inferSchema=True)
    print('Q1:', air_flights_most_canceled_flights(flights),
          'had the most canceled flights in September 2021.')
    print('Q2:', air_flights_diverted_flights(flights), 'flights were diverted between the period of 20th-30th '
                                                       'November 2021.')
    print('Q3:', air_flights_avg_airtime(flights), 'is the average airtime for flights that were flying from '
                                                   'Nashville to Chicago.')
    print('Q4:', air_flights_missing_departure_time(flights), 'unique dates where departure time (DepTime) was '
                                                              'not recorded.')


if __name__ == '__main__':
    main()
