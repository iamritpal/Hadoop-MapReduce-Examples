

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;			// https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/util/Tool.html
import org.apache.hadoop.util.ToolRunner;


// Problem: we want to analyze input data to check and see where picking
//			up passengers is best for TLC drivers.

// hdfs://localhost:54310/user/amritpal/05_GreenCab/input/ hdfs://localhost:54310/user/amritpal/05_GreenCab/output/

public class GreenCabDriver implements Tool {

	public static final String INP_TABLE_CONF = "custom.inp.table.file";
	
	@Override
	public int run(String[] args) throws Exception {
		String inpDir = args[0];
		String outDir = args[1];
		int nmbOfReducers = Integer.parseInt(args[2]);
		
		// Creates a new Job with no particular Cluster. 
		// A Cluster will be created with a generic Configuration.
		Job job = Job.getInstance();
		
		// Set the Jar by finding where a given class came from.
		job.setJarByClass(GreenCabDriver.class);
		
		// Name the job so it will be easy to find in logs
	    job.setJobName("GreenCabDriver");
		
		// Return the configuration for the job.
		Configuration config = job.getConfiguration();
		config.set(INP_TABLE_CONF, inpDir);

		// Set input format and directory path
		job.setInputFormatClass(TextInputFormat.class);
	    TextInputFormat.addInputPath(job, new Path(inpDir));

	    // Set output format and directory path
	    job.setOutputFormatClass(TextOutputFormat.class);
	    TextOutputFormat.setOutputPath(job, new Path(outDir));
	    
	    // Set mapper and reduce classes for the mapreduce job
	    job.setMapperClass(GreenCabMapper.class);
	    job.setReducerClass(GreenCabReducer.class);
	    
	    // Set number of reducers
	    job.setNumReduceTasks(nmbOfReducers);
	    
	    // Execute job, wait for success or failure and return status
        return job.waitForCompletion(true) ? 0 : 1;

	}
	
	
	public static void main(String[] args) throws Exception {
		
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), new GreenCabDriver(), args);
        
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
