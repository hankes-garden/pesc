package org.yanglin.mr.lab.ecg;

import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

/** Performs no reduction, writing all input values directly to the output. 
 * @deprecated Use {@link org.apache.hadoop.mapreduce.Reducer} instead.
 */
@Deprecated
public class MyIdentityReducer<K, V>
    extends MapReduceBase implements Reducer<K, V, K, V> {

  /** Writes all keys and values directly to output. */
  public void reduce(K key, Iterator<V> values,
                     OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
    try
	{
    	System.out.println(key.toString());
		while (values.hasNext())
		{
			V val = values.next();
			output.collect(key, val);
		}
	}
	catch (Exception e)
	{
		e.printStackTrace();
	}
  }
	
}