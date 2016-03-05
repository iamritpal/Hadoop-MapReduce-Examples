/*
	Name - Amritpal Singh
	Class - NYIT CSCI-860 Big Data Analysis
	Date - 02/27/2016
	Project Description - Impliment a MapReduce program that is able to run by Hadoop
			on HDFS cluster or node. The program's function is to search for particular 
			information in multiple files and process the information it finds.
	File name - InfoSearchDriver.java
	Driver Class Responsibility - The Driver class first checks the invocation of the command,
				It checks the count of the command-line arguments provided.
				It sets values for the job, including the driver, mapper, and reducer classes used.
				We also define the types for output key and value in the job as Text and FloatWritable respectively.
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;			// https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/util/Tool.html
import org.apache.hadoop.util.ToolRunner;

