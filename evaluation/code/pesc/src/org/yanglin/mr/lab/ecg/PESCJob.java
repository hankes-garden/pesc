/**
 * PESC is a parallel system for clustering ECG stream data
 * 
 * @author yanglin
 * @version 1.0
 */

/**
 * TODO: redefine the naming pattern, reuse code!
 */

package org.yanglin.mr.lab.ecg;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.yanglin.mr.lab.CommonTestJob;
import org.yanglin.mr.lab.SmallFiles2SequenceFileMapper;
import org.yanglin.mr.lab.KMeansMR.KMeansDriver;
import org.yanglin.mr.lab.KMeansMR.KMeansMapper;
import org.yanglin.mr.lab.KMeansMR.KMeansReducer;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

/**
 * The overall driver of PESC
 */
public class PESCJob extends Configured implements Tool
{

	@Override
	public int run(String[] args) throws Exception
	{
		// input check
		if (args.length != CommonUtility.ARG_NUM)
		{
			System.err.printf("Usage: %s [generic options] "
					+ "<scheme> <ECG_data_dir> <ECG_rr_dir> "
					+ "<standard_dir> <intermediate_ourput_dir>"
					+ "<output dir> \n", getClass()
					.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// set conf
		getConf().set(CommonUtility.ATTR_SCHEME, args[0]);
		getConf().set(CommonUtility.ATTR_ECG_DATA_DIR, args[1]);
		getConf().set(CommonUtility.ATTR_RR_DIR, args[2]);
		getConf().set(CommonUtility.ATTR_STANDARD_DIR, args[3]);
		getConf().set(CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR, args[4]);
		getConf().set(CommonUtility.ATTR_OUTPUT_DIR, args[5]);
		
		getConf().set(CommonUtility.ATTR_WORK_ID,
				System.currentTimeMillis() + "");
		CommonUtility.printConfiguration(getConf());

		// common variables
		String scheme = getConf().get(CommonUtility.ATTR_SCHEME);
		String intermediateDir = getConf().get(
				CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);
		String finalOutDir = getConf().get(CommonUtility.ATTR_OUTPUT_DIR);

		// clear intermediate output directory if it already existed
		String intermediatePath = scheme + intermediateDir;
		CommonUtility.deleteHDFSFile(intermediatePath);

		// create output dir if it does not exist
		String outputDir = getConf().get(CommonUtility.ATTR_OUTPUT_DIR)
				+ CommonUtility.OUTPUT_GS_RR;
		CommonUtility.CreateHDFSDir(scheme + outputDir);
		outputDir = getConf().get(CommonUtility.ATTR_OUTPUT_DIR)
				+ CommonUtility.OUTPUT_GS_CF;
		CommonUtility.CreateHDFSDir(scheme + outputDir);

		long timeStamp = System.currentTimeMillis();
		System.out.println("--Begin PESC----");

		// Local clustering
		runLocalClustering();
		System.out.println("--Local clustering finished, time: "
				+ (System.currentTimeMillis() - timeStamp));
		timeStamp = System.currentTimeMillis();

		// global merging
		runGlobalMerging();
		System.out.println("--Global Merging finished, time: "
				+ (System.currentTimeMillis() - timeStamp));
		timeStamp = System.currentTimeMillis();

		// cluster evaluation for GM
		String strGMRRPath = scheme + intermediateDir
				+ CommonUtility.OUTPUT_GM_RR;
		String strGMCFPath = scheme + intermediateDir
				+ CommonUtility.OUTPUT_GM_CF;
		runClusterEvalution(strGMRRPath, strGMCFPath);
		System.out.println("--Cluster Evalution finished, time: "
				+ (System.currentTimeMillis() - timeStamp));
		timeStamp = System.currentTimeMillis();

		// global splitting
		ArrayList<String> resultDirs = runGlobalSplitting();
		System.out.println("--Global Splitting finished, time: "
				+ (System.currentTimeMillis() - timeStamp));
		timeStamp = System.currentTimeMillis();

		// cluster evaluation for GS
		for(String strResultDir : resultDirs)
		{
			String strGSRRPath = strResultDir + CommonUtility.OUTPUT_GS_RR;
			String strGSCFPath = strResultDir + CommonUtility.OUTPUT_GS_CF;
			runClusterEvalution(strGSRRPath, strGSCFPath);
			
			// move result to final output
			String strOutputDir = scheme + finalOutDir;
			CommonUtility.move2FinalOutput(strResultDir
					+ CommonUtility.OUTPUT_GS_RR, strOutputDir
					+ CommonUtility.OUTPUT_GS_RR);
			CommonUtility.move2FinalOutput(strResultDir
					+ CommonUtility.OUTPUT_GS_CF, strOutputDir
					+ CommonUtility.OUTPUT_GS_CF);
		}
		if(resultDirs.size() > 0)
		{
			System.out.println("--Final Cluster Evalution finished, time: "
					+ (System.currentTimeMillis() - timeStamp));
		}
		

		return 0;
	}

	/**
	 * Run the local clustering step. This method would read ECG data files and
	 * interpret it to RR according to the original RR files, and then a k-Means
	 * algorithm would be applied to generate clusters. All the RR would be
	 * wrote to intermediate output directory in an manner that all RR belong to
	 * same cluster would be wrote into the same RR file. Besides, the cluster
	 * feature of each cluster would be wrote to HDFS separately.
	 * 
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	private int runLocalClustering() throws IOException
	{
		String scheme = getConf().get(CommonUtility.ATTR_SCHEME);
		String ecgDir = getConf().get(CommonUtility.ATTR_ECG_DATA_DIR);
		String intermediateOutputDir = getConf().get(
				CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);

		// create JobConf
		JobConf jobConf = new JobConf(getConf(), this.getClass());
		jobConf.setJobName("Local Clustering");

		// set path for input and output
		Path inPath = new Path(scheme + ecgDir);
		Path outPath = new Path(scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_LC_RR);
		FileInputFormat.setInputPaths(jobConf, inPath);
		FileOutputFormat.setOutputPath(jobConf, outPath);

		// set format for input and output
		jobConf.setInputFormat(WholeFileInputFormat.class);
		jobConf.setOutputFormat(KeyasNameMSFOutputFormat.class);

		// set class of output key and value
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(RRIntervalWritable.class);

		// set mapper and reducer
		jobConf.setMapperClass(LocalClusteringMapper.class);
		jobConf.setReducerClass(IdentityReducer.class);

		// Reducer Num
		int coreNum = getConf().getInt(
				"mapred.tasktracker.reduce.tasks.maximum", 2);
		
		int clusterNodeNum = new JobClient(jobConf).getClusterStatus().getTaskTrackers();
		int NodeNum = (clusterNodeNum == 0) ? 1 : clusterNodeNum ;
		jobConf.setNumReduceTasks((int) (0.95 * NodeNum * coreNum));

		// run the job
		JobClient.runJob(jobConf);
		return 0;
	}

	private int runGlobalMerging() throws IOException
	{
		String scheme = getConf().get(CommonUtility.ATTR_SCHEME);
		String intermediateOutputDir = getConf().get(
				CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);
		String outputDir = getConf().get(CommonUtility.ATTR_OUTPUT_DIR);

		// create JobConf
		JobConf jobConf = new JobConf(getConf(), this.getClass());
		jobConf.setJobName("Global Merging");

		// generate merging plan
		GlobalMerger merger = new GlobalMerger(jobConf);
		ArrayList<ArrayList<ClusterFeatureWritable>> plan = merger
				.getMergePlan();

		// save merging plan to conf
		for (int i = 0; i < plan.size(); ++i)
		{
			for (int j = 0; j < plan.get(i).size(); ++j)
			{
				String name = CommonUtility.PREFIX_ATTR_MERGING_PLAN
						+ plan.get(i).get(j).rrFileName.toString();
				jobConf.set(name, new String("" + i));
			}
		}

		// set path for input and output
		Path inLCPath = new Path(scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_LC_RR);
		Path inGSPath = new Path(scheme + outputDir
				+ CommonUtility.OUTPUT_GS_RR);

		Path outPath = new Path(scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_GM_RR);
		FileInputFormat.setInputPaths(jobConf, inLCPath, inGSPath);
		FileOutputFormat.setOutputPath(jobConf, outPath);

		// set format for input and output
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(KeyasNameMSFOutputFormat.class);

		// set class of output key and value
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(RRIntervalWritable.class);

		// set mapper and reducer
		jobConf.setMapperClass(GlobalMergingMapper.class);
		jobConf.setReducerClass(GlobalMergingReducer.class);

		// Reducer Num
		int coreNum = getConf().getInt(
				"mapred.tasktracker.reduce.tasks.maximum", 2);
		int clusterNodeNum = new JobClient(jobConf).getClusterStatus().getTaskTrackers();
		int NodeNum = (clusterNodeNum == 0) ? 1 : clusterNodeNum ;
		jobConf.setNumReduceTasks((int) (0.95 * NodeNum * coreNum));

		// run the job
		JobClient.runJob(jobConf);

		return 0;
	}

	private int runClusterEvalution(String strRRDir, String strCFDir)
			throws IOException
	{
		String scheme = getConf().get(CommonUtility.ATTR_SCHEME);
		String intermediateOutputDir = getConf().get(
				CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);

		Configuration conf = new Configuration(getConf());
		conf.set(CommonUtility.ATTR_EV_RR_DIR, strRRDir);
		conf.set(CommonUtility.ATTR_EV_CF_DIR, strCFDir);

		// create JobConf
		JobConf jobConf = new JobConf(conf, this.getClass());
		jobConf.setJobName("Cluster Evaluating");

		// set path for input and output
		Path inGMPath = new Path(strRRDir);
		Path outPath = new Path(scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_EV);
		CommonUtility.deleteHDFSFile(outPath.toString());
		FileInputFormat.setInputPaths(jobConf, inGMPath);
		FileOutputFormat.setOutputPath(jobConf, outPath);

		// set format for input and output
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);

		// set class of output key and value
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(DoubleWritable.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(DoubleWritable.class);

		// set mapper and reducer
		jobConf.setMapperClass(ClusterEvaluationMapper.class);
		jobConf.setReducerClass(ClusterEvaluationReducer.class);

		// Reducer Num
		int coreNum = getConf().getInt(
				"mapred.tasktracker.reduce.tasks.maximum", 2);
		int clusterNodeNum = new JobClient(jobConf).getClusterStatus().getTaskTrackers();
		int NodeNum = (clusterNodeNum == 0) ? 1 : clusterNodeNum ;
		jobConf.setNumReduceTasks((int) (0.95 * NodeNum * coreNum));

		// run the job
		JobClient.runJob(jobConf);

		return 0;
	}

	private ArrayList<String> runGlobalSplitting() throws IOException
	{
		ArrayList<String> resultDirArray = new ArrayList<String>();
		
		// Evaluation current clusters
		String scheme = getConf().get(CommonUtility.ATTR_SCHEME);
		String intermediateOutputDir = getConf().get(
				CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);
		String strOutputDir = scheme
				+ getConf().get(CommonUtility.ATTR_OUTPUT_DIR);

		String strCFPath = scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_GM_CF;
		ArrayList<ClusterFeatureWritable> cfArray = CommonUtility
				.readCFfromFile(strCFPath);
		ClusterEvaluator ev = new ClusterEvaluator(getConf());
		ArrayList<ClusterFeatureWritable> badCFArray = ev
				.getBadClusters(cfArray);

		// copy good CF to final outupt
		cfArray.removeAll(badCFArray);
		for (ClusterFeatureWritable cf : cfArray)
		{

			String strRRFilePath = scheme + intermediateOutputDir
					+ CommonUtility.OUTPUT_GM_RR + cf.rrFileName.toString();
			CommonUtility.move2FinalOutput(strRRFilePath, strOutputDir
					+ CommonUtility.OUTPUT_GS_RR);

			String strCFName = CommonUtility.getCFFileName(cf.rrFileName
					.toString());
			String strCFFilePath = scheme + intermediateOutputDir
					+ CommonUtility.OUTPUT_GM_CF + strCFName;
			CommonUtility.move2FinalOutput(strCFFilePath, strOutputDir
					+ CommonUtility.OUTPUT_GS_CF);
		}

		// run a splitting job for each cluster which need to split
		System.out.println("--" + badCFArray.size()
				+ " clusters need to split----");
		ArrayList<KMeansDriver> threads = new ArrayList<KMeansDriver>();
		for (ClusterFeatureWritable cf : badCFArray)
		{
			// commit a job to submit
			String strFilePath = scheme + intermediateOutputDir
					+ CommonUtility.OUTPUT_GM_RR + cf.rrFileName;
			KMeansDriver kmdriver = new KMeansDriver(getConf(), strFilePath,
					cf.rrFileName.toString(), 2);
			kmdriver.start();
			threads.add(kmdriver);

			System.out.println("--Commited one splitting job----");
		}

		// wait for all the splitting finished
		try
		{
			for (KMeansDriver t : threads)
			{

				t.join();
				String strResultDir = new String(t.getResultDir() );
				if("NULL" == strResultDir)
				{
					continue;
				}
				resultDirArray.add(strResultDir);

			}
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		return resultDirArray;
	}

	public static void main(String[] args) throws Exception
	{
		long start = System.currentTimeMillis();
		int ret = ToolRunner.run(new Configuration(), new PESCJob(), args);

		long time = System.currentTimeMillis() - start;
		System.out.println("PESC finished, cost: " + time + " milliSec.");
		System.exit(ret);

	}

}
