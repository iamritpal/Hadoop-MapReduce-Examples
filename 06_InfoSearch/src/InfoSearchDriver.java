/*
	Name - Amritpal Singh
	Class - NYIT CSCI-860 Big Data Analysis
	Date - 02/27/2016
	Last modified - 04-02-2016
	Project Description - Implement a MapReduce program that is able to run by Hadoop
			on HDFS cluster or node. The program's function is to search for particular 
			information in multiple files and process the information it finds.
	File name - InfoSearchDriver.java
	Driver Class Responsibility - The Driver class first checks the invocation of the command,
				It checks the count of the command-line arguments provided.
				It sets values for the job, including the driver, mapper, and reducer classes used.
				We also define the types for output key and value in the job as Text and FloatWritable respectively.
*/

import org.apache.hadoop.util.Tool;			// https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/util/Tool.html
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InfoSearchDriver implements Tool {
	
    public static final String INP_TABLE_CONF = "custom.inp.table.file";
	
	@Override
	public int run(String[] args) throws Exception {
		String inpDir = args[0];                            // HDFS Input Directory Path
		String outDir = args[1];                            // HDFS Output Directory Path
		int nmbOfReducers = Integer.parseInt(args[2]);      // Also input value for number of reducers
		
		// Creates a new Job with no particular Cluster. 
		// A Cluster will be created with a generic Configuration.
		Job job = Job.getInstance();
		
		// Set the Jar by finding where a given class came from.
		job.setJarByClass(InfoSearchDriver.class);
		
		// Name the job so it will be easy to find in logs
		job.setJobName("InfoSearchDriver");
		
		// Return the configuration for the job.
		Configuration config = job.getConfiguration();
		config.set(INP_TABLE_CONF, inpDir);
		
		// Change the default key/value output separator by setting the property
		config.set("mapreduce.output.textoutputformat.separator", ":");

		// Set input format and directory path
		FileInputFormat.addInputPath(job, new Path(inpDir));
        
		// Set mapper and reduce classes for the mapreduce job
		job.setMapperClass(InfoSearchMapper.class);
		job.setReducerClass(InfoSearchReducer.class);
		
        // Set output format and directory path
        FileOutputFormat.setOutputPath(job, new Path(outDir));
        
        // Set the key, value types expected as output from both the map and reduce phases
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
		// Set number of reducers
		job.setNumReduceTasks(nmbOfReducers);
		
		// Execute job, wait for success or failure and return status
		return job.waitForCompletion(true) ? 0 : 1;

	}
	
	
	public static void main(String[] args) throws Exception {
		
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new InfoSearchDriver(), args);
		
		System.exit(res);
	}
	
	
	// setConf() and getConf() need to implemented if this driver class implements Tool.

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub

	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub

		return null;
	}
	
	
}