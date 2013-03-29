package org.yanglin.mr.lab;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.yanglin.mr.lab.ecg.ClusterFeatureWritable;
import org.yanglin.mr.lab.ecg.CommonUtility;
import org.yanglin.mr.lab.ecg.RRIntervalWritable;

public class CommonTestJob extends Configured implements Tool
{

	public static class CommonTestMapper extends MapReduceBase implements
			Mapper<Text, RRIntervalWritable, Text, RRIntervalWritable>
	{
		private JobConf	conf;

		@Override
		public void configure(JobConf job)
		{
			this.conf = job;
		}

		@Override
		public void map(Text key, RRIntervalWritable value,
				OutputCollector<Text, RRIntervalWritable> output,
				Reporter reporter) throws IOException
		{
			System.out.println(conf.get("map.input.file") + "--"
					+ key.toString() + "--" + value.dataFileName);
		}

	}

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

		// get configuration
		Configuration conf = getConf();
		CommonUtility.printConfiguration(conf);

		// create a JobConf
		JobConf jobconf = new JobConf(conf);

		// set name
		jobconf.setJobName("CommonTestJob");

		// set input/output path
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(jobconf, in);
		FileOutputFormat.setOutputPath(jobconf, out);

		// set input/output format
		jobconf.setInputFormat(SequenceFileInputFormat.class);
		jobconf.setOutputFormat(SequenceFileOutputFormat.class);

		// set output key/value
		jobconf.setOutputKeyClass(Text.class);
		jobconf.setOutputValueClass(RRIntervalWritable.class);

		// set mapper/reducer class
		jobconf.setMapperClass(CommonTestMapper.class);
		jobconf.setReducerClass(IdentityReducer.class);

		// MultipleInputs.addInputPath(jobconf, new
		// Path("hdfs://localhost/work/lab/ecg/rrSeqMulti"),
		// SequenceFileInputFormat.class, CommonTestMapper.class);
		//
		// MultipleInputs.addInputPath(jobconf, new
		// Path("hdfs://localhost/work/lab/ecg/rrSeqSingle"),
		// SequenceFileInputFormat.class, IdentityMapper.class);

		jobconf.setNumReduceTasks(0);

		JobClient.runJob(jobconf);

		// --- end---

		CommonUtility.printConfiguration(jobconf);

		return 0;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		// // delete output
		// boolean bDelete = true;
		// if (bDelete)
		// {
		// Configuration conf = new Configuration();
		// FileSystem fs = FileSystem.get(
		// URI.create("hdfs://localhost/work/lab/output/commonTest"), conf);
		// fs.delete(new Path("/work/lab/output/commonTest"), true);
		// }
		//
		// int ret = ToolRunner
		// .run(new Configuration(), new CommonTestJob(), args);
		// System.exit(ret);
		//
		
		//----output result for evalution--------------
		
//		// read CF
//		String strDir = "hdfs://node10:9000/icc/finalOutput/";
//		String strCFPath = strDir + "CF/";
//		ArrayList<ClusterFeatureWritable> cfs = CommonUtility.readCFfromFile(strCFPath);
//		System.out.println("----CF----");
//		for (int i = 0; i < cfs.size(); ++i) {
//			System.out.println("cluster name:" + cfs.get(i).rrFileName);
//			System.out.println("cluster size: " + cfs.get(i).pointNum);
//			System.out.println("long radius: " + cfs.get(i).longRadius);
//			System.out.printf("total distance: %f \n", cfs.get(i).totalDis);
//			System.out.println("average radius: "
//					+ (cfs.get(i).totalDis / cfs.get(i).pointNum));
//			System.out.println();
//		}
		
		


		// write PT intervals
		Configuration conf = new Configuration();
		String strDir = "hdfs://node10:9000/icc/finalOutput/";
		Path centerPath = new Path(strDir + "RR/RR-GS-1349510530589-RR-GM-1349510530589-1-1");
		FileSystem fs = FileSystem.get(URI.create("hdfs://node10:9000//"), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, centerPath,
				conf);
		System.out.println("---------");
		while (true)
		{
			Text key = new Text();
			RRIntervalWritable value = new RRIntervalWritable();
			
			boolean bRet = reader.next(key, value);
			if (bRet)
			{
				String fileName = value.dataFileName.toString().substring(
						value.dataFileName.toString().lastIndexOf("/") + 1);

				int qrsStart = value.rPos + value.qoPos;
				int end = value.rPos + value.jPos;
				int qrsLen = end - qrsStart;
				int prLen = (int) (qrsLen * CommonUtility.RATIO_PR_QRS);
				int prStart = (value.rPos + value.qoPos) - prLen;
				int steLen = (int) (qrsLen * 2.3d);
				int steStart = (value.rPos + value.jPos);
				int steEnd = steStart + steLen;
				System.out.println(fileName + "," + prStart + "," + steEnd
						+ "," + value.rPos + "," + (value.rPos + value.qoPos));
			}
			else
			{
				break;
			}

		}
		reader.close();
		System.out.println("---------");
	}
}
