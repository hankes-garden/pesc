/**
 * 
 * @author yanglin
 * @version 1.1
 * adapted from Java ML 1.0
 * 
 */

package org.yanglin.mr.lab.ecg;

import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.jets3t.service.model.S3Bucket;

public class KMedoids
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
	public KMedoids(int numberOfClusters, int maxIterations)
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

		// TODO: change to k-medoids++
		// generate medoids randomly.
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
			changed = recalculateMedoids(assignment, clusterFeatures, pointSet);

		}

		if(!changed)
		{
			System.out.println("KMedoids: max iteration number has been reached, count: " + count);
		}
		
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
			double bestDistance = SimilarityMeasurement.calDTWDistance(
					data.get(i).qrs.getData(),
					clusterFeatures[0].center.qrs.getData());

			int bestIndex = 0;
			for (int j = 1; j < clusterFeatures.length; j++)
			{
				double tmpDistance = SimilarityMeasurement.calDTWDistance(
						data.get(i).qrs.getData(),
						clusterFeatures[j].center.qrs.getData());
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
	 * Recalculate the medoids according to the assignment
	 * 
	 * @param assignment
	 *            assignment for current medoids
	 * @param clusterFeatures
	 *            [in] current medoids [out] new medoids
	 * @param data
	 *            the original data set
	 * @return whether the medoids changed
	 */
	private boolean recalculateMedoids(int[] assignment,
			ClusterFeatureWritable[] clusterFeatures,
			ArrayList<RRIntervalWritable> data)
	{
		boolean changed = false;
		for (int i = 0; i < numberOfClusters; i++)
		{
			// generate a cluster
			ArrayList<RRIntervalWritable> cluster = new ArrayList<RRIntervalWritable>();

			for (int j = 0; j < assignment.length; j++)
			{
				if (assignment[j] == i)
				{
					cluster.add(data.get(j));
				}
			}

			// recalculate the medoids
			if (0 == cluster.size()) // new random, empty medoid
			{
				clusterFeatures[i].center = data.get(rg.nextInt(data.size()));
				changed = true;
			}
			else
			{
				ClusterFeatureWritable optMedoid = findOptimalMedoid(cluster,
						clusterFeatures[i]);
				if (null != optMedoid)
				{
					clusterFeatures[i] = optMedoid;
					changed = true;
				}
			}
		}
		return changed;
	}

	private ClusterFeatureWritable findOptimalMedoid(
			ArrayList<RRIntervalWritable> cluster, ClusterFeatureWritable curCF)
	{
//		System.out.println("--Start findOptimalMedoid()--");
		long startTime = System.currentTimeMillis();

		ClusterFeatureWritable optCF = null;
		int counter = 0;
		
		double curTotalDis = (curCF.totalDis == 0.0d) ? Double.MAX_VALUE : curCF.totalDis;
		RRIntervalWritable tmpMedoid = null;
		for (int i = 0; i < cluster.size(); ++i)
		{
			tmpMedoid = cluster.get(i);
			if (tmpMedoid.equals(curCF.center))
			{
				continue;
			}

			double tmpTotalDis = 0.0d;
			double tmpLongRadius = 0.0d;
			for (int j = 0; j < cluster.size(); ++j)
			{
				if (j == i) // same point, no need to calculate distance.
				{
					continue;
				}

				double d = SimilarityMeasurement.calDTWDistance(tmpMedoid.qrs.getData(),
						cluster.get(j).qrs.getData());
				if (d > tmpLongRadius)
				{
					tmpLongRadius = d;
				}

				tmpTotalDis += d;
			}

			if (tmpTotalDis < curTotalDis)
			{
				optCF = new ClusterFeatureWritable();
				optCF.center = tmpMedoid;
				optCF.longRadius = tmpLongRadius;
				optCF.totalDis = tmpTotalDis;
				optCF.pointNum = cluster.size();
				counter = i;

				break; // find optimal medoid, break out
			}
		}
		
		long endTime = System.currentTimeMillis();
		System.out
				.println("Function: findOptimalMedoid() end with a counter:"
						+ counter
						+ ", CF:"
						+ ( (null == optCF) ? "NULL" : optCF.toString()  )
						+ ", cost time: "
						+ (endTime - startTime)
						+ " ms.");

		return optCF;
	}

}
