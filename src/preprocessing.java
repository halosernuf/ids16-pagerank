import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class preprocessing {
	public static class preprocessMap1 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text page = new Text(itr.nextToken());
			//Mark page not a redlink
			context.write(page,new Text("!@#$"));
			while(itr.hasMoreTokens()){
				context.write(new Text(itr.nextToken()), page);
			}
		}
	}
	public static class preprocessReduce1 extends Reducer<Text,Text,Text,Text>{
		private boolean isNotRedLink;
		public void reduce(Text key,  Iterable<Text> values, Context context) throws IOException, InterruptedException{
			StringBuilder sb = new StringBuilder();
			isNotRedLink = false;
			Iterator<Text> it=values.iterator();
			while(it.hasNext()){
				Text value=it.next();
				String val=value.toString();
				if(val.equals("!@#$")){
					isNotRedLink = true;
				}
				sb.append(val);
				sb.append("\t");
			}
			if(isNotRedLink){
				context.write(key, new Text(sb.toString()));
			}
		}
	}
	public static class preprocessMap2 extends Mapper<Object, Text, Text, Text>{
		private Text title = new Text();
		private Text link = new Text();
		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			link.set(itr.nextToken());
			while (itr.hasMoreTokens()) {
				String mark=itr.nextToken();
				if(mark.equals("!@#$")){
					context.write(link, new Text());
				}else{
					title.set(mark);
					context.write(title, link);	
				}
		    }
		}
	}
	
	public static class preprocessReduce2 extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			StringBuilder sb = new StringBuilder();
			Iterator<Text> it=values.iterator();
			sb.append("1.0");
			sb.append("\t");
			while(it.hasNext()){
				Text value=it.next();
				String val=value.toString();
				if(!val.isEmpty()){
					sb.append(val);
					sb.append("\t");
				}
			}
			context.write(key, new Text(sb.toString()));
		}
	}
}
