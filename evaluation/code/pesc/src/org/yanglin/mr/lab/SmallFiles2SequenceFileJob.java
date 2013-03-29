package org.yanglin.mr.lab;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.yanglin.mr.lab.ecg.CommonUtility;
import org.yanglin.mr.lab.ecg.WholeFileInputFormat;

public class SmallFiles2SequenceFileJob extends Configured implements Tool
{

	public static void main(String[] args) throws Exception
	{

		int ret = ToolRunner.run(new SmallFiles2SequenceFileJob(), args);

		System.exit(ret);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception
	{

		if (args.length != 2)
		{
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// create JobConf
		JobConf jobConf = new JobConf(getConf(), this.getClass());
		CommonUtility.printConfiguration(jobConf);

		// set path for input and output
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		// set format for input and output
		jobConf.setInputFormat(WholeFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);

		// set class of output key and value
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(BytesWritable.class);

		// set special attributes
		 jobConf.setNumReduceTasks(2);

		// set mapper and reducer
		jobConf.setMapperClass(SmallFiles2SequenceFileMapper.class);
		jobConf.setReducerClass(IdentityReducer.class);
		
		jobConf.setNumReduceTasks(0);

		// run the job
		JobClient.runJob(jobConf);
		
		

		return 0;

	}

}
