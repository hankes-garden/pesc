package org.yanglin.mr.lab;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SmallFiles2SequenceFileMapper extends MapReduceBase implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

	private JobConf conf;
	
	@Override
	public void configure(JobConf conf)
	{
		this.conf = conf;
	}
	
	@Override
	public void map(NullWritable key, BytesWritable value,
			OutputCollector<Text, BytesWritable> output, Reporter reporter)
			throws IOException {

		String fileName = conf.get("map.input.file");
		System.out.println(fileName);
		output.collect(new Text(fileName), value);
	}

}
