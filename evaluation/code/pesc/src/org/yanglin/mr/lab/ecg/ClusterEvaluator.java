package org.yanglin.mr.lab.ecg;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

public class ClusterEvaluator
{

	private Configuration conf;
	
	public ClusterEvaluator(Configuration conf)
	{
		this.conf = conf;
	}
	
	public ArrayList<ClusterFeatureWritable> getBadClusters(ArrayList<ClusterFeatureWritable> cfArray)
	{
		ArrayList<ClusterFeatureWritable> badCFArray = new ArrayList<ClusterFeatureWritable>();
		
		for(ClusterFeatureWritable cf : cfArray)
		{
			if(isBad(cf) )
			{
				badCFArray.add(cf);
			}
		}
		
		return badCFArray;
	}
	
	public boolean isBad(ClusterFeatureWritable cf)
	{
		double avgDis = cf.totalDis / cf.pointNum;
		double dis = SimilarityMeasurement.calculateRRDistance(cf.center, cf.euclideanCenter);
		double distortion = (0.3*avgDis + 0.7*dis) / (cf.longRadius);
		
//		System.out.println("Distortion: " + distortion
//				+ ", dis:" + dis
//				+ ", avgDis: " + avgDis
//				+ ", longRadius:" + cf.longRadius);
		
		if(distortion >= CommonUtility.MAX_CLUSTER_DISTORTION)
		{
			return true;
		}
		
		return false;
	}

}
