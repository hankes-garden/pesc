package org.yanglin.mr.lab.ecg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class IntsWritable implements Writable
{
	private static final int[]	EMPTY_INTS	= {};

	private int					size;
	private int[]				data;

	public IntsWritable()
	{
		this(EMPTY_INTS);
	}

	public IntsWritable(int[] data)
	{
		this.data = data;
		this.size = data.length;
	}

	public int[] getData()
	{
		return data;
	}

	public int getLength()
	{
		return size;
	}

	public void setDeepCopy(int[] newData)
	{
		this.size = newData.length;
		this.data = new int[size];
		for (int i = 0; i < newData.length; ++i)
		{
			this.data[i] = newData[i];
		}
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(this.size);
		for (int i = 0; i < this.size; ++i)
		{
			out.writeInt(data[i]);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.size = in.readInt();
		this.data = new int[this.size];
		for (int i = 0; i < this.size; ++i)
		{
			data[i] = in.readInt();
		}

	}

}
