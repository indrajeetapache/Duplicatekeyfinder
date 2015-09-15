package duplicateremove;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Duplicateremove extends Configured implements Tool {
	
	 private enum COUNTERS {
		  GOOD,
		  BAD,
		  Total
		 }
	
	public static class Duplicateremovemap extends Mapper<Text, Text, Text, Text>
	{
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException
		{
			context.write(key,value);
		}
	}
	
//	public static class Duplicateremovecombiner extends Reducer<Text, Text, Text, Text>
//	{
//		protected void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
//		{
//			Set<Text> duplicate = new HashSet<Text>();
//			
//			for (Text value : values)
//			{
//				 if (duplicate.add(value)) 
//				 {
//			          context.write(key, value);
//			     }
//			}
//		}
//	}
	
	public static class duplicatereduce extends Reducer<Text, Text, Text, Text>
	{
		protected void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Set<Text> duplicate = new HashSet<Text>();
			  context.getCounter(COUNTERS.Total).increment(1L);
			
			for (Text value : values)
			{
				 if (duplicate.add(value)) {
			          context.write(key, value);
			          context.getCounter(COUNTERS.GOOD).increment(1L);
			        }
				 else 
				 {
					  context.getCounter(COUNTERS.BAD).increment(1L);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new Configuration(), new Duplicateremove(), args));

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Path inputPath = new Path(args[0]);
	    Path outputPath = new Path(args[1]);
	    Configuration conf = new Configuration();
	    
	    Job job = Job.getInstance(conf, "Duplicateremove");
		job.setJarByClass(Duplicateremove.class);
		KeyValueTextInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
	    
	    job.setMapperClass(Duplicateremovemap.class);
	    job.setCombinerClass(duplicatereduce.class);
	    job.setReducerClass(duplicatereduce.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    
	    Counters counters = job.getCounters();
	    System.out.printf("Good: %d, Bad: %dn , Total: %dn",
	    	      counters.findCounter(COUNTERS.GOOD).getValue(),
	    	      counters.findCounter(COUNTERS.BAD).getValue(),
	    	      counters.findCounter(COUNTERS.Total).getValue());
	    	 

	    
	    job.setNumReduceTasks(1);
	    return job.waitForCompletion(true) ? 0 : 1;
	    
	 
		
	}

}


