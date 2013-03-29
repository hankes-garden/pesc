package org.yanglin.mr.lab;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.yanglin.mr.lab.ecg.KeyasNameMSFOutputFormat;
import org.yanglin.mr.lab.ecg.MyIdentityReducer;
import org.yanglin.mr.lab.ecg.RRIntervalWritable;

public class WordCounter
{
	public static class WordCounterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, RRIntervalWritable>
	{

		@Override
		public void map(LongWritable arg0, Text arg1,
				OutputCollector<Text, RRIntervalWritable> arg2, Reporter arg3)
				throws IOException
		{
			String[] arrWords = arg1.toString().split(" ");

			for (int i = 0; i < arrWords.length; ++i)
			{
				RRIntervalWritable rr = new RRIntervalWritable();
				int[] pr = new int[50];
				int[] qrs = new int[28];
				int[] ste = new int[78];
				rr.setPR(pr);
				rr.setQRS(qrs);
				rr.setSTE(ste);
				rr.rPos = i;
				rr.setDataFileName("w_" + arrWords[i]);
				rr.setClusteringPrefix("LC");
				rr.clusteringResult = i;
				
				arg2.collect(new Text(rr.dataFileName.toString() + rr.clusteringResult), rr);
			}
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		JobConf conf = new JobConf(WordCounter.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "/" + Long.toString(new Date().getTime() ) ) );
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(KeyasNameMSFOutputFormat.class);
		
		conf.setMapOutputValueClass(Text.class);
		conf.setMapOutputValueClass(RRIntervalWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(RRIntervalWritable.class);
		
		conf.setMapperClass(WordCounterMapper.class);
		conf.setReducerClass(MyIdentityReducer.class);

		JobClient.runJob(conf);
	}
}
