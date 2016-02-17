import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinFilterPartitioner extends Partitioner<Text, Text>{

	/*
	 * (non-Javadoc)
	 * We are using multipart key, therefore we needed to impliment a customer partition,
	 * our multipart key contains join key plus sort flag, we need to partition only by id
	 * so that both records with the same join key will end up in the same reducer regardless
	 * of whether the original arrived from foo or bar.
	 * 
	 * This is where the magic happens
	 */
	
  @Override
  public int getPartition(Text key, Text value, int numberOfReducers) {
    String keyStr = key.toString();
    
    return Math.abs(keyStr.substring(0, keyStr.length() - 2).hashCode()
        % numberOfReducers);
  }

}