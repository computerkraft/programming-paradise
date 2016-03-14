# programming-paradise
This repository has Map Reduce programs, Java Programs, Pig and Hive scripts/queries.

List of folders having Map Reduce Programs and program description. Every Map Reduce folder has the java source files, class files, 
input files and result jar files.

DISTRIBUTEDGREP:	MapReduce program that searches for occurrences of a given string
INVERTEDINDEX1:	MapReduce program that uses an inverted index mapping.
DISTRIBUTEDGREPCOMBINER:	Add a combiner to the Distributed Grep MapReduce job.
COMPUTEAVERAGE:	Write a MapReduce application that computes the average of a collection of numbers.
CUSTOMPARTITIONER:	Write a Custom partitioner that equally distributes key/values pair between the Mappers and Reducers.
TOTALORDERPARTITIONER:	Use the TotalOrderPartitioner class to globally sort the output of a MapReduce job.
CUSTOMINPUTFORMAT:	MapReduce job that uses a custom InputFormat.
CUSTOMOPFORMAT:	Write a custom OutputFormat class.
SIMPLEMOVINGAVERAGE:	MapReduce application that computes the simple moving average of a stock traded on the NYSE.

Pig_Exercise: It has queries in Pig Latin for analysis of the whitehouse visits in the year 2015. The whitehouse visits raw csv file is 
in the folder. Premiliminary analysis involved finding the people who visited whitehouse specifically to meet POTUS and then their count.

Hive_Excercise: After the raw whitehouse visits data was loaded to HDFS and preliminary analysis performed using Pig, Hive was used to 
do further analysis such as finding the congressmen who visited POTUS.
