/**
 * 
 * @author yanglin
 * @version 1.1
 * adapted from Java ML 1.0
 * 
 */

package org.yanglin.mr.lab.ecg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.jets3t.service.model.S3Bucket;

public class KMeans
{

	/* Number of clusters to generate */
	private int		numberOfClusters;

	/* Random generator for selection of candidate medoids */
	private Random	rg;

	/* The maximum number of iterations the algorithm is allowed to run. */
	private int		maxIterations;

	/**
	 * Creates a new instance of the k-medoids algorithm with the specified
	 * parameters.
	 * 
	 * @param numberOfClusters
	 *            the number of clusters to generate
	 * @param maxIterations
	 *            the maximum number of iteration the algorithm is allowed to
	 *            run
	 * 
	 */
	public KMeans(int numberOfClusters, int maxIterations)
	{
		super();
		this.numberOfClusters = numberOfClusters;
		this.maxIterations = maxIterations;
		rg = new Random(System.currentTimeMillis());
	}

	/**
	 * cluster on given data
	 * 
	 * @param pointSet
	 *            data to cluster
	 * @return the cluster feature(medoid, avgRadius, longRadius, pointNum)
	 */
	public ClusterFeatureWritable[] cluster(
			ArrayList<RRIntervalWritable> pointSet)
	{
		ClusterFeatureWritable[] clusterFeatures = new ClusterFeatureWritable[numberOfClusters];

		// TODO: change to k-means++
		// generate centers randomly.
		for (int i = 0; i < numberOfClusters; i++)
		{
			clusterFeatures[i] = new ClusterFeatureWritable();

			int random = rg.nextInt(pointSet.size());
			clusterFeatures[i].center = pointSet.get(random);
		}

		// iterations
		boolean changed = true;
		int count = 0;
		while (changed && count < maxIterations)
		{
			++count;
			int[] assignment = assign(clusterFeatures, pointSet);
			changed = recalculateCenters(assignment, clusterFeatures, pointSet);

		}

		System.out.println("KMeans finished, count: " + count
				+ (changed ? " reached the limitation." : ", Converged!") );

		return clusterFeatures;

	}

	/**
	 * Assign all instances from the data set to the medoids.
	 * 
	 * @param clusterFeatures
	 *            candidate medoids
	 * @param data
	 *            the data to assign to the medoids
	 * @return best cluster indices for each instance in the data set
	 */
	private int[] assign(ClusterFeatureWritable[] clusterFeatures,
			ArrayList<RRIntervalWritable> data)
	{
		int[] assignment = new int[data.size()];

		for (int i = 0; i < data.size(); i++)
		{
			// find best assignment
			double bestDistance = SimilarityMeasurement.calculateRRDistance(data.get(i), 
					clusterFeatures[0].center);
			
			int bestIndex = 0;
			for (int j = 1; j < clusterFeatures.length; j++)
			{
				double tmpDistance = SimilarityMeasurement.calculateRRDistance(
						data.get(i),
						clusterFeatures[j].center);
				if (tmpDistance < bestDistance)
				{
					bestDistance = tmpDistance;
					bestIndex = j;
				}
			}
			assignment[i] = bestIndex;

			// change result in RR
			data.get(i).clusteringResult = bestIndex;
		}

		return assignment;
	}

	/**
	 * Recalculate the center according to the assignment
	 * 
	 * @param assignment
	 *            assignment for current center
	 * @param clusterFeatures
	 *            [in] current center [out] new center
	 * @param data
	 *            the original data set
	 * @return whether the center changed
	 */
	private boolean recalculateCenters(int[] assignment,
			ClusterFeatureWritable[] clusterFeatures,
			ArrayList<RRIntervalWritable> data)
	{
		boolean changed = false;
		for (int i = 0; i < numberOfClusters; i++)
		{
			// generate a group
			ArrayList<RRIntervalWritable> cluster = new ArrayList<RRIntervalWritable>();

			for (int j = 0; j < assignment.length; j++)
			{
				if (assignment[j] == i)
				{
					cluster.add(data.get(j));
				}
			}

			// recalculate the means
			if (0 == cluster.size()) // new random, empty medoid
			{
				clusterFeatures[i].center = data.get(rg.nextInt(data.size()));
				changed = true;
			}
			else
			{
				ClusterFeatureWritable optCenter = findOptimalCenter(cluster, clusterFeatures[i]);
				if (null != optCenter)
				{
					clusterFeatures[i] = optCenter;
					changed = true;
				}
			}
		}
		return changed;
	}

	private ClusterFeatureWritable findOptimalCenter(
			ArrayList<RRIntervalWritable> cluster, ClusterFeatureWritable curCF)
	{
		ClusterFeatureWritable optCF = null;

		// calculate means
		int[] avgPR = new int[cluster.get(0).pr.getData().length];
		int[] avgQRS = new int[cluster.get(0).qrs.getData().length];
		int[] avgSTE = new int[cluster.get(0).ste.getData().length];
		for (int i = 0; i < cluster.size(); ++i)
		{
			CommonUtility.arrayAdd(avgPR, cluster.get(i).pr.getData());
			CommonUtility.arrayAdd(avgQRS, cluster.get(i).qrs.getData());
			CommonUtility.arrayAdd(avgSTE, cluster.get(i).ste.getData());
		}
		CommonUtility.arrayDivide(avgPR, cluster.size());
		CommonUtility.arrayDivide(avgQRS, cluster.size());
		CommonUtility.arrayDivide(avgSTE, cluster.size());

		// check if it is unchanged
		RRIntervalWritable tmpRR = new RRIntervalWritable();
		tmpRR.pr.setDeepCopy(avgPR);
		tmpRR.qrs.setDeepCopy(avgQRS);
		tmpRR.ste.setDeepCopy(avgSTE);

		double d = SimilarityMeasurement.calculateRRDistance(tmpRR, curCF.center);
		if (d < CommonUtility.CONVERGE_ERROR)
		{
			return null;
		}

		// found new center, update center information
		optCF = new ClusterFeatureWritable();
		optCF.center = new RRIntervalWritable();
		optCF.center.setPR(avgPR);
		optCF.center.setQRS(avgQRS);
		optCF.center.setSTE(avgSTE);
		optCF.pointNum = cluster.size();
		double totalDis = 0.0d;
		double longRadius = 0.0d;
		for (int i = 0; i < cluster.size(); ++i)
		{
			double dis = SimilarityMeasurement.calculateRRDistance(
					optCF.center, cluster.get(i) );
			totalDis += dis;
			if (dis > longRadius)
			{
				longRadius = dis;
			}
		}
		optCF.totalDis = totalDis;
		optCF.longRadius = longRadius;

		return optCF;
	}
	
	

}
