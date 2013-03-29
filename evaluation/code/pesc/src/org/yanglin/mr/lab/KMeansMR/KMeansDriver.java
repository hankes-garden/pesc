package org.yanglin.mr.lab.KMeansMR;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.net.nntp.NewsgroupInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.yanglin.mr.lab.ecg.ClusterFeatureWritable;
import org.yanglin.mr.lab.ecg.CommonUtility;
import org.yanglin.mr.lab.ecg.KeyasNameMSFOutputFormat;
import org.yanglin.mr.lab.ecg.PESCJob;
import org.yanglin.mr.lab.ecg.RRIntervalWritable;

public class KMeansDriver extends Thread
{
	private Configuration	conf;
	private String			strFilePath;
	private String			strID;
	private int				splitNum;

	private String			strResultDir;

	public KMeansDriver(Configuration conf, String strFilePath, String strID,
			int splitNum)
	{
		this.conf = conf;
		this.strFilePath = strFilePath;
		this.strID = strID;
		this.splitNum = splitNum;
		this.strResultDir = new String("NULL");
	}

	public String getResultDir()
	{
		return this.strResultDir;
	}

	@Override
	public void run()
	{

		try
		{
			// split
			strResultDir = runSplittingJob(strFilePath, strID, splitNum);
			System.out.println(this.getId() + "finished, dir: " + strResultDir);

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

	}

	public String runSplittingJob(String strFilePath, String strID, int splitNum)
			throws IOException
	{
		String scheme = conf.get(CommonUtility.ATTR_SCHEME);
		String intermediateOutputDir = conf
				.get(CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);
		String strWorkDir = scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_GS_SPLIT_TMP + strID + "/";

		// seeding
		ArrayList<ClusterFeatureWritable> newCenters = seeding(strFilePath,
				splitNum);

		// write all the centers to HDFS
		String strCFPath = strWorkDir + CommonUtility.OUTPUT_GS_CENTER;

		Path cfPath = new Path(strCFPath);
		FileSystem fs = FileSystem.get(URI.create(strCFPath), conf);
		fs.delete(cfPath, true); // delete previous centers
		SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, cfPath,
				IntWritable.class, ClusterFeatureWritable.class);
		for (int i = 0; i < newCenters.size(); ++i)
		{
			out.append(new IntWritable(i), newCenters.get(i));
		}
		IOUtils.closeStream(out);

		// iteration
		int maxIteration = CommonUtility.MAX_ITERATION_GS;
		int count = 0;
		long converged = 0;
		do
		{
			// set conf
			conf.set(CommonUtility.ATTR_KMMR_WORK_DIR, strWorkDir);
			String counterGp = strID;
			conf.set(CommonUtility.ATTR_KMMR_COUNTER_GROUP, counterGp);
			conf.set(CommonUtility.ATTR_KMMR_SID, strID);
			conf.set(CommonUtility.ATTR_KMMR_K, splitNum + "");
			String strCurDepDir = strWorkDir + "dep_" + count + "/";
			conf.set(CommonUtility.ATTR_KMMR_WORK_DIR_DEP_DIR, strCurDepDir);

			// create JobConf
			JobConf jobConf = new JobConf(conf, this.getClass());
			jobConf.setJobName("Global Splitting: " + strID + ", dep_" + count);

			// set path
			String strInDir = strWorkDir + "dep_" + (count - 1) + "/"
					+ CommonUtility.OUTPUT_GS_RR;
			if (0 == count) // 1st iteration, use input file path
			{
				strInDir = strFilePath;
			}

			String strOutDir = strCurDepDir + CommonUtility.OUTPUT_GS_RR;
			FileInputFormat.setInputPaths(jobConf, strInDir);
			FileOutputFormat.setOutputPath(jobConf, new Path(strOutDir));

			// set format
			jobConf.setInputFormat(SequenceFileInputFormat.class);
			jobConf.setOutputFormat(KeyasNameMSFOutputFormat.class);

			// set mapper key value
			jobConf.setMapOutputKeyClass(Text.class);
			jobConf.setMapOutputValueClass(RRIntervalWritable.class);

			// set output key value
			jobConf.setOutputKeyClass(Text.class);
			jobConf.setOutputValueClass(RRIntervalWritable.class);

			// set mapper reducer
			jobConf.setMapperClass(KMeansMapper.class);
			jobConf.setReducerClass(KMeansReducer.class);

			// Reducer No. must be 1
			jobConf.setNumReduceTasks(1);

			// run
			RunningJob job = JobClient.runJob(jobConf);
			converged = job.getCounters()
					.findCounter(counterGp, CommonUtility.COUNTER_CONVERGE)
					.getCounter();

			++count;

		} while ((0 == converged) && (count < maxIteration));

		// iteration ends
		System.out.println("K-Means MR-" + strID + "finished, count: " + count);

		String strResultPath = strWorkDir + "dep_" + (count - 1) + "/";

		return strResultPath;
	}

	public ArrayList<ClusterFeatureWritable> seeding(String strFilePath, int num)
			throws IOException
	{
		ArrayList<ClusterFeatureWritable> centerArray = new ArrayList<ClusterFeatureWritable>();
		SequenceFile.Reader reader = null;

		try
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(strFilePath), conf);
			int fileLen = (int) fs.getLength(new Path(strFilePath));

			reader = new SequenceFile.Reader(fs, new Path(strFilePath), conf);

			Random rg = new Random(System.currentTimeMillis());
			while (centerArray.size() < num)
			{
				int random = rg.nextInt(fileLen);
				reader.sync(random);

				Text key = new Text();
				RRIntervalWritable value = new RRIntervalWritable();
				if (reader.next(key, value))
				{
					ClusterFeatureWritable cf = new ClusterFeatureWritable();
					cf.center = value;
					centerArray.add(cf);
				}
			}
		}
		finally
		{
			IOUtils.closeStream(reader);
		}

		System.out.println("Seeding suceed~!");
		return centerArray;
	}
}
