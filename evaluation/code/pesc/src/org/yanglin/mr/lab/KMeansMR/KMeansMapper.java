package org.yanglin.mr.lab.KMeansMR;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.yanglin.mr.lab.ecg.ClusterFeatureWritable;
import org.yanglin.mr.lab.ecg.CommonUtility;
import org.yanglin.mr.lab.ecg.RRIntervalWritable;
import org.yanglin.mr.lab.ecg.SimilarityMeasurement;

/**
 * This mapper is used for assigning data to its nearest center.
 * Its input is a pair of center and RR, and output the nearest center and RR
 * @author yanglin
 *
 */
public class KMeansMapper extends MapReduceBase	implements
		Mapper<Text, RRIntervalWritable, Text, RRIntervalWritable>
{
	private final List<ClusterFeatureWritable>	centerArray	= new ArrayList<ClusterFeatureWritable>();
	private JobConf								conf;

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
			e.printStackTrace();
		}
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
				centerArray.add(value);
			}
			else
			{
				break;
			}
			
		}
		reader.close();
	}

	@Override
	public void map(Text key, RRIntervalWritable value,
			OutputCollector<Text, RRIntervalWritable> output,
			Reporter reporter) throws IOException
	{

		// assign RR to its nearest center
		int  assign = Integer.MAX_VALUE;
		double nearestDistance = Double.MAX_VALUE;
		for (int i = 0; i < centerArray.size(); ++i)
		{
			ClusterFeatureWritable cf = centerArray.get(i);
			double dist = SimilarityMeasurement.calculateRRDistance(cf.center, value);

			if (nearestDistance > dist)
			{
				assign = i;
				nearestDistance = dist;
			}
		}

		String wID = conf.get(CommonUtility.ATTR_WORK_ID);
		String sID = conf.get(CommonUtility.ATTR_KMMR_SID);
		String strNewKey = CommonUtility.PREFIX_TYPE_RR
				+ CommonUtility.COMMON_SEPARATOR
				+ CommonUtility.PREFIX_STEP_GS
				+ CommonUtility.COMMON_SEPARATOR
				+ wID
				+ CommonUtility.COMMON_SEPARATOR
				+ sID
				+ CommonUtility.COMMON_SEPARATOR
				+ assign;
		
		output.collect(new Text(strNewKey), value);
	}

}
