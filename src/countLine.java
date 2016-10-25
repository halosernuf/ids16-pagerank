import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class countLine {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("Total Lines");
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
                context.write(word,one);
        }
     }
     public static class Reduce extends Reducer<Text, IntWritable, IntWritable,NullWritable> {
    	 	private IntWritable result = new IntWritable();
    	 	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
              int sum = 0;
              for (IntWritable val : values) {
                  sum += val.get();
                }
              result.set(sum);
              context.write(result,NullWritable.get());
            }
     }
}
