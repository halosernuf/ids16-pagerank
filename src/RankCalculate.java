import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class RankCalculate {
	
	public static class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text>{
		 
	    @Override
	    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	int totalLink;
	    	String fromPage=itr.nextToken();
	    	String pageRank=itr.nextToken();
	    	totalLink=itr.countTokens();
	    	StringBuilder sb=new StringBuilder();
	    	while(itr.hasMoreTokens()){
	    		String token = itr.nextToken();
	    		sb.append(token);
	    		sb.append("\t");
	    		context.write(new Text(token),new Text(pageRank+"\t"+totalLink));
	    	}
	    	context.write(new Text(fromPage),new Text("|"+sb.toString()));
	    }
	}
	public static class RankCalculateReduce extends Reducer<Text, Text, Text, Text> {
	    private static final float damping = 0.85F;
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        float sumShareOtherPageRanks = 0;
	        String links="";
	        for(Text val:values){
	        	if(val.toString().startsWith("|")){
	        		links=val.toString().substring(1);
	        		continue;
	        	}
	        	StringTokenizer itr = new StringTokenizer(val.toString());
	        	float pageRank= Float.valueOf(itr.nextToken());
	        	int outLinks = Integer.valueOf(itr.nextToken());
	        	sumShareOtherPageRanks += (pageRank/outLinks);
	        }
	        float newRank = damping * sumShareOtherPageRanks + (1-damping);
	        context.write(key, new Text(newRank+"\t"+links));
	    }
	}

}
