package org.yanglin.mr.lab.ecg;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import javax.print.attribute.standard.SheetCollate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GlobalMerger
{
	private Configuration	conf;

	public GlobalMerger(Configuration conf)
	{
		this.conf = conf;
	}

	/**
	 * @param scheme
	 * @param outputDir
	 * @param dataFileName
	 * @throws IOException
	 */

	public ArrayList<ArrayList<ClusterFeatureWritable>> getMergePlan()
			throws IOException
	{
		String scheme = conf.get(CommonUtility.ATTR_SCHEME);
		String intermediateOutputDir = conf.get(CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);
		String OutputDir = conf.get(CommonUtility.ATTR_OUTPUT_DIR);

		Configuration conf = new Configuration();

		// read global CF
		String globalSCFPath = scheme + OutputDir
				+ CommonUtility.OUTPUT_GS_CF;
		ArrayList<ClusterFeatureWritable> globalSCF = CommonUtility.readCFfromFile(globalSCFPath);

		// read local CF
		String localCFPath = scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_LC_CF;
		ArrayList<ClusterFeatureWritable> localCF = CommonUtility.readCFfromFile(localCFPath);

		// merge
		return this.generateMergingPlan(localCF, globalSCF);
	}
	
	/**
	 * Generate merge plan, every CF in the same sub-plan would be merged as one cluster
	 * @param localCFArray
	 * @param globalSCFArray
	 * @return
	 */
	private ArrayList<ArrayList<ClusterFeatureWritable>> generateMergingPlan(ArrayList<ClusterFeatureWritable> localCFArray, 
			ArrayList<ClusterFeatureWritable> globalSCFArray)
	{
		ArrayList<ArrayList<ClusterFeatureWritable>> mergingPlan = new ArrayList<ArrayList<ClusterFeatureWritable>>();
		
		// add global SCF to plan
		for (int i = 0; i < globalSCFArray.size(); ++i)
		{
			ArrayList<ClusterFeatureWritable> subPlan = new ArrayList<ClusterFeatureWritable>();
			subPlan.add(globalSCFArray.get(i) );
			mergingPlan.add(subPlan);

		}
		
		// merge local CF to plan
		for(int m = 0; m < localCFArray.size(); ++m)
		{
			ClusterFeatureWritable cf = localCFArray.get(m);
			
			boolean bMerged = false;
			for(int i = 0; i < mergingPlan.size(); ++i)
			{
				for(int j= 0; j < (mergingPlan.get(i)).size(); ++j)
				{
					ClusterFeatureWritable tmpCF = mergingPlan.get(i).get(j);
					
					double conn = this.calConnectivity(cf, tmpCF);
					if(CommonUtility.MAX_CLUSTER_CONN <= conn)
					{
						// add to this sub-plan
						mergingPlan.get(i).add(cf);
						bMerged = true;
						break;
					}
				}
				
				if(bMerged)
				{
					break;
				}
			}
			
			if(!bMerged) // could not be merging with the others, new GM Cluster
			{
				ArrayList<ClusterFeatureWritable> subPlan = new ArrayList<ClusterFeatureWritable>();
				subPlan.add(cf);
				mergingPlan.add(subPlan);
			}
		}
		
		return mergingPlan;
	}
	
	/**
	 * calculate the connectivity btw two clusters.
	 * @param c1
	 * @param c2
	 * @return	connectivity
	 */
	private double calConnectivity(ClusterFeatureWritable c1, ClusterFeatureWritable c2)
	{
		double avgR1 = c1.totalDis / (double)c1.pointNum;
		double avgR2 = c2.totalDis / (double)c2.pointNum;
		double centerDis = SimilarityMeasurement.calculateRRDistance(c1.center, c2.center);
		double conn = (avgR1 + avgR2) / centerDis;
//		System.out.println("Conn: " + conn);
		return conn;
	}
	
}
