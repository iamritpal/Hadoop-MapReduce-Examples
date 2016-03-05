
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InfoSearchMapper extends
	Mapper<LongWritable, Text, Text, IntWritable> {		// k1, v1, k2, v2

	static String name = "Singh,Amritpal";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			
			// Check if token contains substring with searchKeyword
            if (token.contains(name)) {
            	
            	// Split token into array 
            	String[] tokenArray  = token.split(":");
            	
            	// Validate input
            	if ((tokenArray.length == 2) && 				// tokenArray length must be 2, if separated by ":"
            		(tokenArray[0].equals(name)) && 			// tokenArray[0] must be the name
            		(StringUtils.isNumeric(tokenArray[1])))		// tokenArray[1] must be numeric value
            	{
            		int nameValue = Integer.parseInt(tokenArray[1]);
            		context.write(new Text(name), new IntWritable(nameValue));
            	}
            }
        }
	}	
}