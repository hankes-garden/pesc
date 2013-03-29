package org.yanglin.mr.lab.ecg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class GlobalMergingMapper extends MapReduceBase implements Mapper<Text, RRIntervalWritable, Text, RRIntervalWritable>
{
	private JobConf	conf;

	@Override
	public void configure(JobConf conf)
	{
		this.conf = conf;
	}

	@Override
	public void map(Text key, RRIntervalWritable value,
			OutputCollector<Text, RRIntervalWritable> output, Reporter reporter)
			throws IOException
	{
		String strRRFilePath = conf.get("map.input.file");
		int startIndex = strRRFilePath.lastIndexOf("/")+1;
		String strRRFileName = strRRFilePath.substring(startIndex);
		
		// read merging plan
		String mergingPlan = conf.get(CommonUtility.PREFIX_ATTR_MERGING_PLAN + strRRFileName);
		if (null == mergingPlan)
		{
			throw new IOException("Could not find merging plan for " + strRRFileName);
		}
		
		// change the clustering result of RR
		value.clusteringPrefix = new Text(CommonUtility.PREFIX_STEP_GM);
		value.clusteringResult = Integer.parseInt(mergingPlan);
		
		// change key to merging key
		String wID = conf.get(CommonUtility.ATTR_WORK_ID);
		String strMergedFileName = CommonUtility.PREFIX_TYPE_RR
				+ CommonUtility.COMMON_SEPARATOR
				+ CommonUtility.PREFIX_STEP_GM
				+ CommonUtility.COMMON_SEPARATOR
				+ wID
				+ CommonUtility.COMMON_SEPARATOR
				+ mergingPlan;
		Text mergingKey = new Text(strMergedFileName);
		
		output.collect(mergingKey, value);
	}

}
