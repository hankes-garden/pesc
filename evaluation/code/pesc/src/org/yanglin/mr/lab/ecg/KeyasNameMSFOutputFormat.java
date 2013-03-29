package org.yanglin.mr.lab.ecg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;

public class KeyasNameMSFOutputFormat extends
		MultipleSequenceFileOutputFormat<Text, RRIntervalWritable>
{

	protected String generateFileNameForKeyValue(Text key,
			RRIntervalWritable value, String name)
	{
		String strName = new String(key.toString());
		return strName;
	}
}
