package org.yanglin.mr.lab.ecg;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.eclipse.jdt.internal.core.JavaModelManager.EclipsePreferencesListener;

/**
 * This GlobalMergingReducer just calculate the CF for 
 * the new cluster generated by global merging. However, 
 * some CF attributes, say totalDis and longRadius  are 
 * not update since they could not  be computed in one 
 * iteration fashion of reducer.
 * @author yanglin
 *
 */
public class GlobalMergingReducer extends MapReduceBase implements
		Reducer<Text, RRIntervalWritable, Text, RRIntervalWritable>
{
	private Configuration conf;
	
	@Override
	public void configure(JobConf conf)
	{
		this.conf = conf;
	}

	@Override
	public void reduce(Text key, Iterator<RRIntervalWritable> values,
			OutputCollector<Text, RRIntervalWritable> output, Reporter reporter)
			throws IOException
	{
		int clusterSize = 0;

		// calculate means & euclideanCenter
		RRIntervalWritable firstRR = values.next();
		int prLen = firstRR.pr.getData().length;
		int qrsLen = firstRR.qrs.getData().length;
		int steLen = firstRR.ste.getData().length;
		
		// mean
		int[] avgPR = new int[prLen];
		int[] avgQRS = new int[qrsLen];
		int[] avgSTE = new int[steLen];
		CommonUtility.arrayAdd(avgPR, firstRR.pr.getData());
		CommonUtility.arrayAdd(avgQRS, firstRR.qrs.getData());
		CommonUtility.arrayAdd(avgSTE, firstRR.ste.getData());
		
		// Euclidean center
		int[] eucliPR = new int[prLen];
		int[] eucliPRMin = new int[prLen];
		CommonUtility.arraySet(eucliPRMin, Integer.MAX_VALUE);
		int[] eucliPRMax = new int[prLen];
		
		int[] eucliQRS = new int[qrsLen];
		int[] eucliQRSMin = new int[qrsLen];
		CommonUtility.arraySet(eucliQRSMin, Integer.MAX_VALUE);
		int[] eucliQRSMax = new int[qrsLen];
		
		int[] eucliSTE = new int[steLen];
		int[] eucliSTEMin = new int[steLen];
		CommonUtility.arraySet(eucliSTEMin, Integer.MAX_VALUE);
		int[] eucliSTEMax = new int[steLen];
		
		CommonUtility.arrayFindMinMax(firstRR.pr.getData(), eucliPRMax, eucliPRMin);
		CommonUtility.arrayFindMinMax(firstRR.qrs.getData(), eucliQRSMax, eucliQRSMin);
		CommonUtility.arrayFindMinMax(firstRR.ste.getData(), eucliSTEMax, eucliSTEMin);
		
				
		++clusterSize;
		output.collect(key, firstRR);

		while (values.hasNext())
		{
			RRIntervalWritable rr = values.next();
			
			// mean
			CommonUtility.arrayAdd(avgPR, rr.pr.getData());
			CommonUtility.arrayAdd(avgQRS, rr.qrs.getData());
			CommonUtility.arrayAdd(avgSTE, rr.ste.getData());
			
			// Euclidean center
			CommonUtility.arrayFindMinMax(rr.pr.getData(), eucliPRMax, eucliPRMin);
			CommonUtility.arrayFindMinMax(rr.qrs.getData(), eucliQRSMax, eucliQRSMin);
			CommonUtility.arrayFindMinMax(rr.ste.getData(), eucliSTEMax, eucliSTEMin);
			
			++clusterSize;
			
			// out put
			output.collect(key, rr);
		}
		
		// center
		CommonUtility.arrayDivide(avgPR, clusterSize);
		CommonUtility.arrayDivide(avgQRS, clusterSize);
		CommonUtility.arrayDivide(avgSTE, clusterSize);
		
		// Euclidean Center
		CommonUtility.arrayAdd(eucliPR, eucliPRMax);
		CommonUtility.arrayAdd(eucliPR, eucliPRMin);
		CommonUtility.arrayDivide(eucliPR, 2);

		CommonUtility.arrayAdd(eucliQRS, eucliQRSMax);
		CommonUtility.arrayAdd(eucliQRS, eucliQRSMin);
		CommonUtility.arrayDivide(eucliQRS, 2);

		CommonUtility.arrayAdd(eucliSTE, eucliSTEMax);
		CommonUtility.arrayAdd(eucliSTE, eucliSTEMin);
		CommonUtility.arrayDivide(eucliSTE, 2);
		
		// new CF
		ClusterFeatureWritable cf = new ClusterFeatureWritable();
		cf.center.setPR(avgPR);
		cf.center.setQRS(avgQRS);
		cf.center.setSTE(avgSTE);
		cf.euclideanCenter.setPR(eucliPR);
		cf.euclideanCenter.setQRS(eucliQRS);
		cf.euclideanCenter.setSTE(eucliSTE);
		cf.pointNum = clusterSize;
		cf.rrFileName = key;
		
		
		// write CF to file
		String scheme = conf.get(CommonUtility.ATTR_SCHEME);
		String intermediateOutputDir = conf
				.get(CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);
		String wID = conf.get(CommonUtility.ATTR_WORK_ID);
		String strDirPath = scheme + intermediateOutputDir + CommonUtility.OUTPUT_GM_CF;
		String strPrefix = CommonUtility.PREFIX_TYPE_CF
				+ CommonUtility.COMMON_SEPARATOR 
				+ CommonUtility.PREFIX_STEP_GM
				+ CommonUtility.COMMON_SEPARATOR
				+ wID
				+ CommonUtility.COMMON_SEPARATOR
				+ firstRR.clusteringResult;

		ClusterFeatureWritable[] cfArray = new ClusterFeatureWritable[1];
		cfArray[0] = cf;
		CommonUtility.writeCF2File(cfArray, strDirPath, strPrefix);
	}

}