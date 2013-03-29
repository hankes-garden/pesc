/**
 * The basic unit for processing
 * 
 * @author yanglin
 * 
 */

package org.yanglin.mr.lab.ecg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

// A example of RR File in JSON
// {"BaseY":0,"DataInfoId":1,"DataLength":176,"DetailId":1,"Duration":0,"
// EndTime":"\/Date(1346947218000+0800)\/", //"Flag":1048576,"FormatString":null,
// "Frequency":300,"HR":102,"JPos":15,"JPosValue":796,"ParentType":1,"QStartPos":-12,"RPos":521,
// "STDuration":0,"STPos":38,"STValue":0,"StartTime":"\/Date(1346947218000+0800)\/","SubType":2},

public class RRIntervalWritable implements Writable
{
	public int					rPos			= 0;	// R point
	public short				qoPos			= 0;	// Qo
	public short				jPos			= 0;	// J
	public short				baselineValue	= 0;	// baseline
	public short				dataFreq		= 0;	// data frequency
	public int					refFlag			= 0;	// a flag for reference
	public byte					refType			= 0;	// a type for reference
	public short				refSubType		= 0;	// a sub-type for
	public int					clusteringResult;		// clustering result
	public Text					dataFileName;			// corresponding data
	public Text					clusteringPrefix;		// clustering prefix
	public IntsWritable			qrs;					// QRS data
	public IntsWritable			pr;						// from the start point of p to the start point of Q wave
	public IntsWritable			ste;					// from the end point of S wave to the end point of T wave

	public RRIntervalWritable()
	{
		init();
	}
	
	public RRIntervalWritable(RRIntervalWritable rr)
	{
		init();
		rPos = rr.rPos;
		qoPos = rr.qoPos;
		jPos = rr.jPos;
		baselineValue = rr.baselineValue;
		dataFreq = rr.dataFreq;
		refFlag = rr.refFlag;
		refType	= rr.refType;
		refSubType	= rr.refSubType;
		dataFileName = new Text(rr.dataFileName);
		clusteringResult = rr.clusteringResult;
		clusteringPrefix = new Text(rr.clusteringPrefix);
		this.setQRS(rr.qrs.getData() );
		this.setPR(rr.pr.getData() );
		this.setSTE(rr.ste.getData() );

	}

	public boolean equals(RRIntervalWritable rr)
	{
		boolean ret = false;
		if (this.dataFileName.toString().equals(rr.dataFileName.toString())
				&& this.rPos == rr.rPos)
		{
			ret = true;
		}

		return ret;
	}

	public RRIntervalWritable(JSONObject obj)
	{
		try
		{
			init();

			this.rPos = obj.getInt("RPos");
			this.qoPos = (short) obj.getInt("QStartPos");
			this.jPos = (short) obj.getInt("STPos");
			this.baselineValue = (short) obj.getInt("BaseY");
			this.dataFreq = (short) obj.getInt("Frequency");
			this.refFlag = obj.getInt("Flag");
			this.refType = (byte) obj.getInt("ParentType");
			this.refSubType = (short) obj.getInt("SubType");

		}
		catch (JSONException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	private void init()
	{
		this.clusteringPrefix = new Text(CommonUtility.PREFIX_STEP_LC);
		this.dataFileName = new Text("");
		this.qrs = new IntsWritable();
		this.pr = new IntsWritable();
		this.ste = new IntsWritable();
	}

	/**
	 * Read QRS from source. NOTE: each point is represented in two bytes!
	 * 
	 * @param srcBytes
	 *            source data
	 * @param start
	 * 
	 * @param len
	 */
	public void setQRS(byte[] srcBytes, int start, int len)
	{
		if( (start < 0) 
				|| 
				( ( (start+len-1)*2 +1) >= srcBytes.length) )
		{
			System.out.println("Invalid para for setQRS: start: " + start + ", len: " + len);
			return;
		}
		
		int[] tmpData = new int[len];

		for (int i = 0; i < len; ++i)
		{
			tmpData[i] = (short) ((srcBytes[(i + start) * 2 + 1] << 8) | srcBytes[(i + start) * 2]);
		}

		this.qrs.setDeepCopy(tmpData);
	}
	
	/**
	 * Read PR from source. NOTE: each point is represented in two bytes!
	 * 
	 * @param srcBytes
	 *            source data
	 * @param start
	 * 
	 * @param len
	 */
	public void setPR(byte[] srcBytes, int start, int len)
	{
		if( (start < 0) 
				|| 
				( ( (start+len-1)*2 +1) >= srcBytes.length) )
		{
			System.out.println("Invalid para for setPR: start: " + start + ", len: " + len);
			return;
		}
		
		int[] tmpData = new int[len];

		for (int i = 0; i < len; ++i)
		{
			tmpData[i] = (short) ((srcBytes[(i + start) * 2 + 1] << 8) | srcBytes[(i + start) * 2]);
		}

		this.pr.setDeepCopy(tmpData);
	}
	
	/**
	 * Read STE from source. NOTE: each point is represented in two bytes!
	 * 
	 * @param srcBytes
	 *            source data
	 * @param start
	 * 
	 * @param len
	 */
	public void setSTE(byte[] srcBytes, int start, int len)
	{
		if( (start < 0) 
				|| 
				( ( (start+len-1)*2 +1) >= srcBytes.length) )
		{
			System.out.println("Invalid para for setSTE: start: " + start + ", len: " + len);
			return;
		}
		
		int[] tmpData = new int[len];

		for (int i = 0; i < len; ++i)
		{
			tmpData[i] = (short) ((srcBytes[(i + start) * 2 + 1] << 8) | srcBytes[(i + start) * 2]);
		}

		this.ste.setDeepCopy(tmpData);
	}
	
	public void setPR(int[] data)
	{
		this.pr.setDeepCopy(data);
	}
	
	public void setQRS(int[] data)
	{
		this.qrs.setDeepCopy(data);
	}
	
	public void setSTE(int[] data)
	{
		this.ste.setDeepCopy(data);
	}

	public void setDataFileName(String fileName)
	{
		this.dataFileName.set(fileName);
	}

	public void setCluteringResult(int result)
	{
		this.clusteringResult = result;
	}

	public void setClusteringPrefix(String strPrefix)
	{
		this.clusteringPrefix.set(strPrefix);
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeInt(rPos);
		out.writeShort(qoPos);
		out.writeShort(jPos);
		out.writeShort(baselineValue);
		out.writeShort(dataFreq);
		out.writeInt(refFlag);
		out.writeByte(refType);
		out.writeShort(refSubType);
		dataFileName.write(out);
		out.writeInt(clusteringResult);
		clusteringPrefix.write(out);
		qrs.write(out);
		pr.write(out);
		ste.write(out);
	}

	public void readFields(DataInput in) throws IOException
	{
		this.rPos = in.readInt();
		this.qoPos = in.readShort();
		this.jPos = in.readShort();
		this.baselineValue = in.readShort();
		this.dataFreq = in.readShort();
		this.refFlag = in.readInt();
		this.refType = in.readByte();
		this.refSubType = in.readShort();
		dataFileName.readFields(in);
		this.clusteringResult = in.readInt();
		clusteringPrefix.readFields(in);
		this.qrs.readFields(in);
		this.pr.readFields(in);
		this.ste.readFields(in);

	}

	public static RRIntervalWritable read(DataInput in) throws IOException
	{
		RRIntervalWritable w = new RRIntervalWritable();
		w.readFields(in);
		return w;
	}

	@Override
	public String toString()
	{
		return new String("RR@" + this.dataFileName + "_" + this.rPos);
	}

}
