import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiPageRanking {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		WikiPageRanking pageRanking = new WikiPageRanking();
		pageRanking.runPreprocessing(args[0], args[1]);
		pageRanking.runCountline(args[0], args[1]);
		int iterate=0;
		for(;iterate<8;iterate++){
			pageRanking.runCalPageRank(args[1], iterate);
		}
		
	}
	
	public void runPreprocessing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "preprocess1");
		job.setJarByClass(preprocessing.class);
		job.setMapperClass(preprocessing.preprocessMap1.class);
		job.setReducerClass(preprocessing.preprocessReduce1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath+"/temp/prepro1"));
	    job.waitForCompletion(true);
	    
	    Job job2 = Job.getInstance(conf, "preprocess2");
	    job2.setJarByClass(preprocessing.class);
	    job2.setMapperClass(preprocessing.preprocessMap2.class);
		job2.setReducerClass(preprocessing.preprocessReduce2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(outputPath+"/temp/prepro1"));
	    FileOutputFormat.setOutputPath(job2, new Path(outputPath+"/temp/prepro2"));
	    job2.waitForCompletion(true);
	}
	
	public void runCountline(String inputPath,String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "countline");
		job.setJarByClass(countLine.class);
		job.setMapperClass(countLine.Map.class);
		job.setReducerClass(countLine.Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(NullWritable.class);	    
	    
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath+"/temp/countline"));
	    job.waitForCompletion(true);
	}
	
	public void runCalPageRank(String outputPath,int iterate) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "calPageRank");
		job.setJarByClass(RankCalculate.class);
		
		job.setMapperClass(RankCalculate.RankCalculateMapper.class);
		job.setReducerClass(RankCalculate.RankCalculateReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		if(iterate==0){
			FileInputFormat.addInputPath(job, new Path(outputPath+"/temp/prepro2"));
		}else{
			FileInputFormat.addInputPath(job, new Path(outputPath+"/temp/iterate/"+iterate));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath+"/temp/iterate/"+(iterate+1)));
		job.waitForCompletion(true);
	}

}
