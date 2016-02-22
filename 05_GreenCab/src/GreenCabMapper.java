
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GreenCabMapper extends
	Mapper<LongWritable, Text, Text, DoubleWritable> {		// k1, v1, k2, v2

	static int lineCounter = 0;
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (lineCounter != 0) {
			String line = value.toString();
			String[] cells = StringUtils.split(line, ",");
			
			Double tipAmount;
			tipAmount = Double.parseDouble(cells[14]);
			
			String[] pickupDateAndTime = cells[1].split(" ");
			String date = pickupDateAndTime[0];
			context.write(new Text(date), new DoubleWritable(tipAmount));
		}
		else {
			lineCounter++;			
		}
	}
	

}
