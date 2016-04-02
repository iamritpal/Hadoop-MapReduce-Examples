
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InfoSearchReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {		// <k2, v2, k3, v3>
																// k2 = reducer key input
																// v2 = reducer list of value input for the key
																// k3 = reducer key output
																// v3 = reducer value output

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
		throws IOException, InterruptedException {
	
		int sum = 0;		// Declare and initialize sum integer with value 0
		
		// For the given key iterate through all the values and add up to sum variable
		for (IntWritable value : values) {
	    	sum += value.get();
	    }
	    context.write(key, new IntWritable(sum));
		
	}
}
