package com.pl.traffic.s1.sum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.WritableComparable;
 

/**
 * @author max400
 *
 */
public class trafficBean implements WritableComparable<trafficBean>  {

	private String phone ;
	private long upSize;
	private long downSize;
	private long sumSize;
	
	
	public trafficBean() { }

	public trafficBean(String phone, long upSize, long downSize) { 
		this.phone = phone;
		this.upSize = upSize;
		this.downSize = downSize; 
		this.sumSize = this.upSize + downSize; 
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public long getUpSize() {
		return upSize;
	}

	public void setUpSize(long upSize) {
		this.upSize = upSize;
	}

	public long getDownSize() {
		return downSize;
	}

	public void setDownSize(long downSize) {
		this.downSize = downSize;
	}

	public long getSumSize() {
		return sumSize;
	}

	public void setSumSize(long sumSize) {
		this.sumSize = sumSize;
	}

	/**
	 * 将对象序列化到流中
	 */
	public void write(DataOutput out) throws IOException {
		 
		out.writeUTF(phone);
		out.writeLong(upSize);
		out.writeLong(downSize);
		out.writeLong(sumSize);
	}

	
	/**
	 * 从流中反序列化出数据
	 * 和write 顺序要一致
	 */
	public void readFields(DataInput in) throws IOException {
		phone = in.readUTF();
		upSize = in.readLong();
		downSize  = in.readLong();
		sumSize  = in.readLong();
	}

	@Override
	public String toString() {
		 
		return 	upSize
				+ "\t" + downSize
				+ "\t" + sumSize;
	}
 
	//对合计排序
	public int compareTo(trafficBean o) { 
		//默认:  〉 返回 1   ,= 返回0 ， < 返回 -1
		
		//-1 倒序
		return sumSize>o.getSumSize()?-1:1;
	}
}
