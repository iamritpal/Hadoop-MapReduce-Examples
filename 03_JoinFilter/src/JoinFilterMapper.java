import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinFilterMapper extends 
  Mapper<LongWritable, Text, Text, Text> {

  boolean isFooBlock = false;
  int fooValFilter;

  public static final int FOO_ID_INX = 0;
  public static final int FOO_VALUE_INX = 1;
  public static final int FOO_BAR_ID_INX = 2;
  public static final int BAR_ID_INX = 0;
  public static final int BAR_VALUE_INX = 1;

  //static int test=0;
  
  Text newKey = new Text();
  Text newValue = new Text();

  @Override
  public void setup(Context context) {		// setup() read predefined values from config object
	  
    //A
    Configuration config = context.getConfiguration();
    fooValFilter = config.getInt(JoinFilterExampleMRJob.FOO_VAL_MAX_CONF, -1);
    
    //B		// later in map() we will use this info to tell whether we working with foo or bar file
    String fooRootPath = config.get(JoinFilterExampleMRJob.FOO_TABLE_CONF);
    FileSplit split = (FileSplit) context.getInputSplit();		// check file split's path to see if file input foo or bar
    if (split.getPath().toString().contains(fooRootPath)) {
      isFooBlock = true;
    }
  }

  // file input is feed to map func line by line via value
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] cells = StringUtils.split(value.toString(), "|");
    
    
    //System.out.println("test = " + test++);
    //System.out.println(value.toString());
    
    //C
    if (isFooBlock) {		// Foo file block
      int fooValue = Integer.parseInt(cells[FOO_VALUE_INX]);

      if (fooValue <= fooValFilter) {
        newKey.set(cells[FOO_BAR_ID_INX] + "|" + JoinFilterExampleMRJob.FOO_SORT_FLAG);
        newValue.set(cells[FOO_ID_INX] + "|" + cells[FOO_VALUE_INX]);
        //D
        context.write(newKey, newValue);
      } else {
        //E 		// useful debug information for the developer
        context.getCounter("Custom", "FooValueFiltered").increment(1);
      }
    } else {
      newKey.set(cells[BAR_ID_INX] + "|" + JoinFilterExampleMRJob.BAR_SORT_FLAG);
      newValue.set(cells[BAR_VALUE_INX]);
      context.write(newKey, newValue);
    }
  }
}