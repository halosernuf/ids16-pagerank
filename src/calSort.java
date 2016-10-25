import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class calSort {
	
	public static class calSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			Configuration conf = context.getConfiguration();
			Double num_nodes = Double.valueOf(conf.get("num_nodes"));
			String page=itr.nextToken();
			Double pagerank=Double.valueOf(itr.nextToken());
			if(pagerank>=5f){
				context.write(new DoubleWritable(pagerank/num_nodes),new Text(page));
			}
		}

	}
	public static class DescendingKeyComparator extends WritableComparator {
	    protected DescendingKeyComparator() {
	    	super(DoubleWritable.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	DoubleWritable key1 = (DoubleWritable) w1;
	    	DoubleWritable key2 = (DoubleWritable) w2;          
	        return -1 * key1.compareTo(key2);
	    }
	}
	public static class calSortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val:values){
				context.write(val, key);
			}
		}
	}
}
