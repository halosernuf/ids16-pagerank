import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiPageRanking {
	static String num_nodes;
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		WikiPageRanking pageRanking = new WikiPageRanking();
        Configuration conf = new Configuration();
        FileSystem fs =  FileSystem.get(new URI(args[1]), conf);
        HashMap<String,String> map = new HashMap<String,String>();
		map.put("input", args[0]);
        map.put("preprocess1", args[1]+"/temp/preprocess1");
        map.put("preprocess2", args[1]+"/temp/preprocess2");
        map.put("pageCount", args[1]+"/temp/coutline");
        map.put("num_nodes", args[1]+"/num_nodes");
        map.put("iterate", args[1]+"/temp/iterate/");
        map.put("rank", args[1]+"/temp/rank/");
        map.put("output", args[1]+"/iter");
        //preprocessing remove redlinks
        pageRanking.runPreprocessing(map.get("input"),map.get("preprocess1"),map.get("preprocess2"));
		//get number of pages and write to num_nodes
		pageRanking.runCountline(map.get("input"),map.get("pageCount"));
		Path path = new Path(map.get("pageCount"));
		FileUtil.copyMerge(fs,path,fs,new Path(map.get("num_nodes")),true, conf, null);
		//read num_nodes from file
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(map.get("num_nodes")))));
        num_nodes = br.readLine();
        //iterate 8 times and output rank
        int iterate=1;
		for(;iterate<=8;iterate++){
			if(iterate==1){
				pageRanking.runCalPageRank(map.get("preprocess2"),map.get("iterate")+iterate);
			}else{
				pageRanking.runCalPageRank(map.get("iterate")+(iterate-1),map.get("iterate")+iterate);
			}
			if(iterate==1 || iterate==8){
				pageRanking.runCalSort(map.get("iterate")+iterate, map.get("rank")+iterate);
				Path srcpath=new Path(map.get("rank")+iterate);
				Path dstpath=new Path(map.get("output")+iterate+".out");
				FileUtil.copyMerge(fs,srcpath,fs,dstpath,true, conf, null);
			}
		}
		
		
	}
	
	public void runPreprocessing(String inputPath, String outputPath1,String outputPath2) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "preprocess1");
		job.setJarByClass(preprocessing.class);
		job.setMapperClass(preprocessing.preprocessMap1.class);
		job.setReducerClass(preprocessing.preprocessReduce1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath1));
	    job.waitForCompletion(true);
	    
	    Job job2 = Job.getInstance(conf, "preprocess2");
	    job2.setJarByClass(preprocessing.class);
	    job2.setMapperClass(preprocessing.preprocessMap2.class);
		job2.setReducerClass(preprocessing.preprocessReduce2.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(outputPath1));
	    FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
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
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    job.waitForCompletion(true);
	}
	
	public void runCalPageRank(String inputPath,String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "calPageRank");
		job.setJarByClass(RankCalculate.class);
		
		job.setMapperClass(RankCalculate.RankCalculateMapper.class);
		job.setReducerClass(RankCalculate.RankCalculateReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	public void runCalSort(String inputPath,String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		conf.set("num_nodes", num_nodes);
		Job job = Job.getInstance(conf, "calSort");
		job.setJarByClass(calSort.class);
		
		job.setMapperClass(calSort.calSortMapper.class);
		job.setReducerClass(calSort.calSortReducer.class);
		
		job.setSortComparatorClass(calSort.DescendingKeyComparator.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
}
