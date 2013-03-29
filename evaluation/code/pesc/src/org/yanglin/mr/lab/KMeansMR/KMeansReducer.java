package org.yanglin.mr.lab.KMeansMR;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.yanglin.mr.lab.ecg.ClusterFeatureWritable;
import org.yanglin.mr.lab.ecg.CommonUtility;
import org.yanglin.mr.lab.ecg.RRIntervalWritable;
import org.yanglin.mr.lab.ecg.SimilarityMeasurement;

/**
 * This Reducer is used for calculate the center of clusters
 * It read in a pair of a center and data assigned to it, then output the new center of cluster and RR 
 * @author yanglin
 *
 */
public class KMeansReducer extends MapReduceBase
		implements
		Reducer<Text, RRIntervalWritable, Text, RRIntervalWritable>
{
	private final List<ClusterFeatureWritable>	curCenters	= new ArrayList<ClusterFeatureWritable>();
	private final List<ClusterFeatureWritable>	newCenters	= new ArrayList<ClusterFeatureWritable>();
	private JobConf								conf;

	public static enum Counter
	{
		CONVERGED
	}
	
	private void init(Configuration conf) throws IOException
	{

		String scheme = conf.get(CommonUtility.ATTR_SCHEME);
		String workDir = conf.get(CommonUtility.ATTR_KMMR_WORK_DIR);
		String strCFPath = workDir + CommonUtility.OUTPUT_GS_CENTER;
		
		Path centerPath = new Path(strCFPath);
		FileSystem fs = FileSystem.get(URI.create(scheme), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, centerPath,
				conf);
		while(true)
		{
			IntWritable key = new IntWritable();
			ClusterFeatureWritable value = new ClusterFeatureWritable();
			boolean bRet = reader.next(key, value);
			if(bRet)
			{
				curCenters.add(value);
			}
			else
			{
				break;
			}
			
		}
		reader.close();
	}

	@Override
	public void configure(JobConf conf)
	{
		this.conf = conf;
		
		try
		{
			init(conf);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException
	{
		// write all the centers to HDFS
		String scheme = conf.get(CommonUtility.ATTR_SCHEME);
		String workDir = conf.get(CommonUtility.ATTR_KMMR_WORK_DIR);
		String strCFPath = workDir + CommonUtility.OUTPUT_GS_CENTER;
		
		Path cfPath = new Path(strCFPath);
		FileSystem fs = FileSystem.get(URI.create(scheme), conf);
		fs.delete(cfPath, true); // delete previous centers
		final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
				cfPath, IntWritable.class, ClusterFeatureWritable.class);
		for (int i = 0; i < newCenters.size(); ++i)
		{
			out.append(new IntWritable(i), newCenters.get(i));
		}
		out.close();

	}

	@Override
	public void reduce(Text key, Iterator<RRIntervalWritable> values,
			OutputCollector<Text, RRIntervalWritable> output,
			Reporter reporter) throws IOException
	{
		// calculate means
		int clusterSize = 0;
		RRIntervalWritable fistrRR = values.next();
		int[] avgPR = new int[fistrRR.pr.getData().length];
		int[] avgQRS = new int[fistrRR.qrs.getData().length];
		int[] avgSTE = new int[fistrRR.ste.getData().length];

		CommonUtility.arrayAdd(avgPR, fistrRR.pr.getData());
		CommonUtility.arrayAdd(avgQRS, fistrRR.qrs.getData());
		CommonUtility.arrayAdd(avgSTE, fistrRR.ste.getData());
		++clusterSize;
		
		output.collect(key, fistrRR);

		while (values.hasNext())
		{
			RRIntervalWritable rr = values.next();
			CommonUtility.arrayAdd(avgPR, rr.pr.getData());
			CommonUtility.arrayAdd(avgQRS, rr.qrs.getData());
			CommonUtility.arrayAdd(avgSTE, rr.ste.getData());
			++clusterSize;

			// out put
			output.collect(key, rr);
		}

		// update CF of this cluster(NOTE: some information is not updated, or even initialized!)
		CommonUtility.arrayDivide(avgPR, clusterSize);
		CommonUtility.arrayDivide(avgQRS, clusterSize);
		CommonUtility.arrayDivide(avgSTE, clusterSize);

		ClusterFeatureWritable optCF = new ClusterFeatureWritable();
		optCF.center.setPR(avgPR);
		optCF.center.setQRS(avgQRS);
		optCF.center.setSTE(avgSTE);
		optCF.pointNum = clusterSize;

		// write local CF to file
		String strDirPath = conf.get(CommonUtility.ATTR_KMMR_WORK_DIR_DEP_DIR) + CommonUtility.OUTPUT_GS_CF;
		int index = key.toString().indexOf(CommonUtility.COMMON_SEPARATOR);
		String strPrefix =  CommonUtility.PREFIX_TYPE_CF + key.toString().substring(index);

		ClusterFeatureWritable[] cfs = new ClusterFeatureWritable[1];
		cfs[0] = optCF;
		CommonUtility.writeCF2File(cfs, strDirPath, strPrefix);
		
		int start = key.toString().lastIndexOf(CommonUtility.COMMON_SEPARATOR);
		int centerIndex = Integer.parseInt(key.toString().substring(start+1) );
		double d = SimilarityMeasurement.calculateRRDistance(optCF.center,	curCenters.get(centerIndex).center);
		if (d < CommonUtility.CONVERGE_ERROR) // converged!
		{
			String counterGroup = conf.get(CommonUtility.ATTR_KMMR_COUNTER_GROUP);
			reporter.incrCounter(counterGroup, CommonUtility.COUNTER_CONVERGE, 1);
		}

		// add to global centers
		newCenters.add(optCF);
	}
}
