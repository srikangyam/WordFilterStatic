package com.skangyam.hadoop.mapreduce.WordFilterStatic;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordFilterStDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordFilterStDriver(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] "
					+ "<input dir> <output Directory> [State By default CA]\n",
					getClass().getSimpleName());
			return -1;
		}
		
		//Step 1: Create the Job
		Job job = new Job(getConf());
		
		//Step 2: Set Jar by Class to detect the Jar with the specified classname
		job.setJarByClass(WordFilterStDriver.class);

		//Step 3: Set the Job Name
		job.setJobName(this.getClass().getName());
		
		//Step 4: Control the Reducers. In this situation, as we do not need to combine the output
		//        at the reducer, we would set the value to 0
		job.setNumReduceTasks(0);
		
		//Step 5: Set the Input and Output File formats
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Step 6: Set the Map class
		job.setMapperClass(WordFilterStMapper.class);
		
		//Step 7: Set Maps key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Step 8: Run the job and wait for completion
		if (job.waitForCompletion(true)) {
			return 0;
		}
		
		return 1;
	}
}
