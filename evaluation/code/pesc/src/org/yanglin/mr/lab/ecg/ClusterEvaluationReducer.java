package org.yanglin.mr.lab.ecg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ClusterEvaluationReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable>
{

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values,
			OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException
	{
		double totalDis = 0.0d;
		double longRadius = 0.0d;
		while(values.hasNext() )
		{
			double dis = values.next().get();
			
			totalDis += dis;
			if(dis > longRadius)
			{
				longRadius = dis;
			}
		}
		
		//write back to CF
		ArrayList<ClusterFeatureWritable> cfArray = CommonUtility.readCFfromFile(key.toString() );
		ClusterFeatureWritable cf = cfArray.get(0);
		cf.totalDis = totalDis;
		cf.longRadius = longRadius;
		
		ClusterFeatureWritable[] cfs = new ClusterFeatureWritable[1];
		cfs[0] = cf;
		
		CommonUtility.deleteHDFSFile(key.toString() );//delete first
		int index = key.toString().lastIndexOf("/");
		String dir = key.toString().substring(0, index+1);
		String prefix = key.toString().substring(index + 1);
		CommonUtility.writeCF2File(cfs, dir, prefix);
		
		System.out.println("Evaluation finished.");
		
	}

}
