package org.yanglin.mr.lab.ecg;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ClusterEvaluationMapper extends MapReduceBase implements Mapper<Text, RRIntervalWritable, Text, DoubleWritable>
{
	private JobConf conf;
	
	ArrayList<ClusterFeatureWritable> cfArray = null;
	
	public void configure(JobConf conf)
	{
		this.conf = conf;
		
		try
		{
			Init();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void Init() throws IOException
	{
		// read corresponding CF file
		String cfFilePath = conf.get(CommonUtility.ATTR_EV_CF_DIR);
		this.cfArray = CommonUtility.readCFfromFile(cfFilePath);
	}

	@Override
	public void map(Text key, RRIntervalWritable value,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException
	{
		ClusterFeatureWritable cf = null;
		for(int i = 0; i < cfArray.size(); ++i)
		{
			if(key.toString().trim().equals(cfArray.get(i).rrFileName.toString().trim()))
			{
				cf = cfArray.get(i);
				break;
			}
		}
		if(null == cf)
		{
			System.out.println("Error: could not find corresponding CF");
			return;
		}
		
		// calculate
		double dis = SimilarityMeasurement.calculateRRDistance(value, cf.center);
		
		// output
		String cfFilePath = conf.get(CommonUtility.ATTR_EV_CF_DIR)+ CommonUtility.getCFFileName(key.toString() );
		output.collect(new Text(cfFilePath), new DoubleWritable(dis));
		
	}

}
