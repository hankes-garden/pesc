/**
 * This file is part of the Java Machine Learning Library
 * 
 * The Java Machine Learning Library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * The Java Machine Learning Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with the Java Machine Learning Library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 * 
 * Copyright (c) 2006-2012, Thomas Abeel
 * 
 * Project: http://java-ml.sourceforge.net/
 * 
 */
package org.yanglin.mr.lab.ecg;

import java.awt.Point;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.jetty.EofException;

/**
 * A similarity measure based on "Dynamic Time Warping". The DTW distance is
 * mapped to a similarity measure using f(x)= 1 - (x / (1 + x)). Feature weights
 * are also supported.
 * 
 * @author Piotr Kasprzak
 * @author Thomas Abeel
 * 
 */
public class SimilarityMeasurement
{

	public static int pointDistance(int i, int j, int[] ts1, int[] ts2)
	{
		int diff = ts1[i] - ts2[j];
		return (diff * diff);
	}

	public static double distance2Similarity(double x)
	{
		return (1.0 - (x / (1 + x)));
	}
	
	public static double calDTWDistance(int[] x, int[] y)
	{
		ArrayList<Point> dtwPath = new ArrayList<Point>();
		return SimilarityMeasurement.calDTWDistance(x, y, dtwPath);
		
	}

	public static double calDTWDistance(int[] x, int[] y, ArrayList<Point> dtwPath)
	{

		/** Check for some special cases due to ultra short time series */
		if (x.length == 0 || y.length == 0)
		{
			return Double.NaN;
		}

		/** Build a point-to-point distance matrix */
		int[][] dP2P = new int[x.length][y.length];
		int i, j;
		for (i = 0; i < x.length; i++)
		{
			for (j = 0; j < y.length; j++)
			{
				dP2P[i][j] = pointDistance(i, j, x, y);
			}
		}

		if (x.length == 1 && y.length == 1)
		{
			return (Math.sqrt(dP2P[0][0]));
		}

		/**
		 * Build the optimal distance matrix using a dynamic programming
		 * approach
		 */
		int[][] D = new int[x.length][y.length];

		D[0][0] = dP2P[0][0]; // Starting point

		for (i = 1; i < x.length; i++)
		{ // Fill the first column of our
			// distance matrix with optimal
			// values
			D[i][0] = dP2P[i][0] + D[i - 1][0];
		}

		if (y.length == 1)
		{ // TS2 is a point
			double sum = 0;
			for (i = 0; i < x.length; i++)
			{
				sum += D[i][0];
			}
			return (Math.sqrt(sum) / x.length);
		}

		for (j = 1; j < y.length; j++)
		{ // Fill the first row of our
			// distance matrix with optimal
			// values
			D[0][j] = dP2P[0][j] + D[0][j - 1];
		}

		if (x.length == 1)
		{ // TS1 is a point
			double sum = 0;
			for (j = 0; j < y.length; j++)
			{
				sum += D[0][j];
			}
			return (Math.sqrt(sum) / y.length);
		}

		for (i = 1; i < x.length; i++)
		{ // Fill the rest
			for (j = 1; j < y.length; j++)
			{
				int[] steps = { D[i - 1][j - 1], D[i - 1][j], D[i][j - 1] };
				int min = Math.min(steps[0], Math.min(steps[1], steps[2]));
				D[i][j] = dP2P[i][j] + min;
			}
		}

		/**
		 * Calculate the distance between the two time series through optimal
		 * alignment.
		 */
		i = x.length - 1;
		j = y.length - 1;
		int k = 1;
		double dist = D[i][j];
		dtwPath.add(new Point(i, j));

		while (i + j > 2)
		{
			if (i == 0)
			{
				j--;
			}
			else if (j == 0)
			{
				i--;
			}
			else
			{
				double[] steps = { D[i - 1][j - 1], D[i - 1][j], D[i][j - 1] };
				double min = Math.min(steps[0], Math.min(steps[1], steps[2]));

				if (min == steps[0])
				{
					i--;
					j--;
				}
				else if (min == steps[1])
				{
					i--;
				}
				else if (min == steps[2])
				{
					j--;
				}
			}
			k++;
			dist += D[i][j];

			dtwPath.add(new Point(i, j));
		}
		dtwPath.add(new Point(0, 0));

		return (Math.sqrt(dist) / k);
	}

	/**
	 * align src according to ref
	 * @param src
	 * @param ref
	 * @return
	 */
	public static int[] alignbyDTW(int[] src, int[] ref)
	{
		int[] aligned = new int[ref.length];
		ArrayList<Point> dtwPath = new ArrayList<Point>();

		// get dtw path
		SimilarityMeasurement.calDTWDistance(src, ref, dtwPath);

		// align
		int index = 0;
		int tmpSum = 0;
		int tmpCount = 0;
		
		for (int m = (dtwPath.size() - 1); m >= 0; --m)
		{
			int x = dtwPath.get(m).x;
			int y = dtwPath.get(m).y;
			
			if (m ==0) //last node in path
			{
				if(tmpSum != 0)
				{
					tmpSum += src[x];
					++tmpCount;
					
					int avg = (int)(tmpSum/tmpCount);
					aligned[index] = avg;
					++index;
					
					tmpSum = 0;
					tmpCount = 0;
					
					continue;
				}
				
				aligned[index] = src[x];
				break;
			}
			
			int nextX = dtwPath.get(m-1).x;
		    int nextY = dtwPath.get(m-1).y;
			
			if(y == nextY) // re-sample
			{
				tmpSum += src[x];
				++tmpCount;

				continue;
			}
			if(tmpSum != 0)
			{
				tmpSum += src[x];
				++tmpCount;
				
				int avg = (int)(tmpSum/tmpCount);
				aligned[index] = avg;
				++index;
				
				tmpSum = 0;
				tmpCount = 0;
				
				continue;
			}
			
			if(x == nextX) // multi-sample
			{
				aligned[index] = src[x];
				++index;
			}

			if( (x!=nextX) && (y!=nextY) )
			{
				aligned[index] = src[x];
				++index;
				
				continue;
			}
			
		}

		return aligned;
	}

	public static double CalEuclideanDis(int[] x, int[] y)
	{
		
		if(x.length != y.length)
		{
			System.out.println("ERROR: Unequivalent lenght of input, can not calculate Euclidean distance correctly.");
			return Double.MAX_VALUE;
		}
		
		double dis = 0;
		for(int i = 0; i < x.length; ++i)
		{
			dis += SimilarityMeasurement.pointDistance(i, i, x, y);
		}
		
		dis = Math.sqrt(dis) / x.length;
		
		return dis;
	}
	
	public static double calculateRRDistance(RRIntervalWritable r1, RRIntervalWritable r2)
	{
		if(r1.pr.getData().length != r2.pr.getData().length
				|| r1.qrs.getData().length != r2.qrs.getData().length
				|| r1.ste.getData().length != r2.ste.getData().length)
		{
			System.out.println("ERROR: unequavilent lenght of data, align first!");
			return Double.MAX_VALUE;
		}
		
		// calculate distance of PR
		double prDis = SimilarityMeasurement.CalEuclideanDis(r1.pr.getData(), r2.pr.getData() );
		double qrsDis = SimilarityMeasurement.CalEuclideanDis(r1.qrs.getData(), r2.qrs.getData() );
		double steDis = SimilarityMeasurement.CalEuclideanDis(r1.ste.getData(), r2.ste.getData() );
		
		
		double totalDis = CommonUtility.WEIGHT_RR_DIS_PR * prDis 
				+ CommonUtility.WEIGHT_RR_DIS_QRS * qrsDis 
				+ CommonUtility.WEIGHT_RR_DIS_STE * steDis;
		
		return totalDis;
	}
	
	public static void main(String[] args) throws Exception
	{
		
	}
}
