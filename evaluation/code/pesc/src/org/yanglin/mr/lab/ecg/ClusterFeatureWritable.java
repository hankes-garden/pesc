/**
 * The representation of a cluster
 * CF = (medoid, avgRadius, longRadius, piontNum)
 * 
 * @author yanglin
 * 
 */
package org.yanglin.mr.lab.ecg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.spec.ECField;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ClusterFeatureWritable implements Writable
{
	public RRIntervalWritable	center			= null;
	public RRIntervalWritable	euclideanCenter	= null;
	public double				totalDis		= 0.0d;
	public double				longRadius		= 0.0d;
	public int					pointNum		= 0;
	public Text					rrFileName;			// corresponding RR file
														// name

	public ClusterFeatureWritable()
	{
		center = new RRIntervalWritable();
		euclideanCenter = new RRIntervalWritable();
		rrFileName = new Text("");
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		center.write(out);
		euclideanCenter.write(out);
		out.writeDouble(totalDis);
		out.writeDouble(longRadius);
		out.writeInt(pointNum);
		rrFileName.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		center.readFields(in);
		euclideanCenter.readFields(in);
		this.totalDis = in.readDouble();
		this.longRadius = in.readDouble();
		this.pointNum = in.readInt();
		this.rrFileName.readFields(in);
	}

	public boolean equals(ClusterFeatureWritable cluster)
	{
		return (this.center.equals(cluster.center));
	}

	@Override
	public String toString()
	{
		return new String("medoid: " + this.center.toString() + ", totalDis: "
				+ this.totalDis + ", longRadius: " + this.longRadius
				+ ", pointNum: " + this.pointNum + ", rrFileName: "
				+ this.rrFileName.toString());
	}

}
