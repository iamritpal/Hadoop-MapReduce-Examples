
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GreenCabReducer
  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {		// k2, v2, k3, v3

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context)
		throws IOException, InterruptedException {
	
		Double maxValue = Double.MIN_VALUE;
		
		for (DoubleWritable value : values) {
	    	maxValue = Math.max(maxValue, value.get());
	    }
	    context.write(key, new DoubleWritable(maxValue));
		
	}
}
