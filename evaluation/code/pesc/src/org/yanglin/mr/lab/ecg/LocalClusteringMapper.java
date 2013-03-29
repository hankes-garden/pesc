package org.yanglin.mr.lab.ecg;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class LocalClusteringMapper extends MapReduceBase implements
		Mapper<NullWritable, BytesWritable, Text, RRIntervalWritable>
{
	private JobConf	conf;

	private int[]	standardPR	= null;
	private int[]	standardQRS	= null;
	private int[]	standardSTE	= null;

	@Override
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

	/**
	 * the output key is the name of RR file which contains this RR interval
	 */
	@Override
	public void map(NullWritable key, BytesWritable value,
			OutputCollector<Text, RRIntervalWritable> output, Reporter reporter)
			throws IOException
	{
		// find corresponding RR list
		String strEcgFileName = conf.get("map.input.file");
		String scheme = conf.get(CommonUtility.ATTR_SCHEME);
		String rrDir = conf.get(CommonUtility.ATTR_RR_DIR);
		String intermediateOutputDir = conf
				.get(CommonUtility.ATTR_INTERMEDIATE_OUTPUT_DIR);

		// read and interpret RR list
		String rrFilePath = scheme + rrDir
				+ CommonUtility.getOriRRFileName(strEcgFileName);
		ArrayList<RRIntervalWritable> rrArray = interpretRRFromDataFile(value,
				strEcgFileName, rrFilePath);
		
		if (CommonUtility.CLUSTER_NUM_LOCAL_CLUSTERING >= rrArray.size())
		{
			// no enough RR to cluster
			System.out.println("ERROR: No enough RR to cluster, number of RR: "
					+ rrArray.size() + ", from data: " + strEcgFileName
					+ ", and rr: " + rrFilePath);
			return;
		}

		// local clustering
		KMeans km = new KMeans(CommonUtility.CLUSTER_NUM_LOCAL_CLUSTERING,
				CommonUtility.MAX_ITERATION_LS);
		ClusterFeatureWritable[] localCF = km.cluster(rrArray);

		// update CF.rrFileName
		for (int i = 0; i < localCF.length; ++i)
		{
			localCF[i].rrFileName = new Text(CommonUtility.PREFIX_TYPE_RR
					+ CommonUtility.COMMON_SEPARATOR
					+ CommonUtility.PREFIX_STEP_LC
					+ CommonUtility.COMMON_SEPARATOR
					+ CommonUtility.getEcgID(strEcgFileName)
					+ CommonUtility.COMMON_SEPARATOR + i);
		}

		// write local CF to file
		String strDirPath = scheme + intermediateOutputDir
				+ CommonUtility.OUTPUT_LC_CF;
		String strPrefix = CommonUtility.PREFIX_TYPE_CF
				+ CommonUtility.COMMON_SEPARATOR + CommonUtility.PREFIX_STEP_LC
				+ CommonUtility.COMMON_SEPARATOR
				+ CommonUtility.getEcgID(strEcgFileName);

		CommonUtility.writeCF2File(localCF, strDirPath, strPrefix);

		// output
		Iterator<RRIntervalWritable> it = rrArray.iterator();
		while (it.hasNext())
		{
			RRIntervalWritable rr = it.next();

			Text outputKey = new Text(CommonUtility.PREFIX_TYPE_RR
					+ CommonUtility.COMMON_SEPARATOR
					+ CommonUtility.PREFIX_STEP_LC
					+ CommonUtility.COMMON_SEPARATOR
					+ CommonUtility.getEcgID(strEcgFileName)
					+ CommonUtility.COMMON_SEPARATOR + rr.clusteringResult);

			output.collect(outputKey, rr);
		}

	}

	/**
	 * @throws IOException
	 */
	private void Init() throws IOException
	{
		// read standard PR, QRS, STE
		String standardDir = conf.get(CommonUtility.ATTR_SCHEME)
				+ conf.get(CommonUtility.ATTR_STANDARD_DIR);
		standardPR = CommonUtility.readStandard(standardDir
				+ CommonUtility.FILE_NAME_STANDARD_PR);
		standardQRS = CommonUtility.readStandard(standardDir
				+ CommonUtility.FILE_NAME_STANDARD_QRS);
		standardSTE = CommonUtility.readStandard(standardDir
				+ CommonUtility.FILE_NAME_STANDARD_STE);
	}

	/**
	 * read and interpret RR list, NOTE: all the RR would be tag as "LC"
	 * 
	 * @param ecgData
	 *            original ECG data
	 * @param dataFileName
	 *            ECG data file name
	 * @param oriRRFilePath
	 *            corresponding RR file Path
	 * @return
	 * @throws IOException
	 */
	private ArrayList<RRIntervalWritable> interpretRRFromDataFile(
			BytesWritable ecgData, String dataFileName, String oriRRFilePath)
			throws IOException
	{
		ArrayList<RRIntervalWritable> rrArray = new ArrayList<RRIntervalWritable>();
		FSDataInputStream in = null;

		try
		{
			// read RR list
			FileSystem fs = FileSystem.get(
					URI.create(conf.get(CommonUtility.ATTR_SCHEME)), conf);
			in = fs.open(new Path(oriRRFilePath));
			byte[] buf = new byte[(int) fs.getLength(new Path(oriRRFilePath))];
			in.readFully(buf);
			String rrListString = new String(buf);

			// interpret
			JSONArray rrJsArray = new JSONArray(rrListString);
			JSONObject jsObj = null;
			int len = rrJsArray.length();
			for (int i = 0; i < len; ++i)
			{
				jsObj = rrJsArray.getJSONObject(i);

				RRIntervalWritable rr = new RRIntervalWritable(jsObj);

				// set corresponding file name
				rr.setDataFileName(dataFileName);

				// set cluster prefix
				rr.setClusteringPrefix(CommonUtility.PREFIX_STEP_LC);

				// set QRS data
				int qrsStart = rr.rPos + rr.qoPos;
				int end = rr.rPos + rr.jPos;
				int qrsLen = end - qrsStart;
				rr.setQRS(ecgData.getBytes(), qrsStart, qrsLen);

				// set PR data
				int prLen = (int) (qrsLen * CommonUtility.RATIO_PR_QRS);
				int prStart = (rr.rPos + rr.qoPos) - prLen;
				rr.setPR(ecgData.getBytes(), prStart, prLen);

				// set STE data
				int steLen = (int) (qrsLen * CommonUtility.RATIO_STE_QRS);
				int steStart = (rr.rPos + rr.jPos);
				rr.setSTE(ecgData.getBytes(), steStart, steLen);

				// align
				int[] alignedPR = SimilarityMeasurement.alignbyDTW(
						rr.pr.getData(), standardPR);
				rr.pr.setDeepCopy(alignedPR);

				int[] alignedQRS = SimilarityMeasurement.alignbyDTW(
						rr.qrs.getData(), standardQRS);
				rr.qrs.setDeepCopy(alignedQRS);

				int[] alignedSTE = SimilarityMeasurement.alignbyDTW(
						rr.ste.getData(), standardSTE);
				rr.ste.setDeepCopy(alignedSTE);

				rrArray.add(rr);
			}

		}
		catch (JSONException e)
		{
			e.printStackTrace();
			System.out.println("Invalid JSON format in " + dataFileName + ", " + oriRRFilePath );
		}
		finally
		{
			IOUtils.closeStream(in);
		}

		return rrArray;
	}

	private void printLocalCF(ClusterFeatureWritable[] localCF)
	{
		String taskID = conf.get("mapred.task.id");
		System.out.println("--Mapper: " + taskID
				+ " has been done, the Local CFs are :---------");
		for (int i = 0; i < localCF.length; ++i)
		{
			System.out.println("LocalCF_" + i + ": "
					+ localCF[i].center.dataFileName + localCF[i].center.rPos);
		}
	}
}
