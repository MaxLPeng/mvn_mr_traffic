package com.pl.traffic.s1.sum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class calcReducer extends Reducer<Text, trafficBean, Text, trafficBean> {
 
	
	/**
	 * 遍历 values ，处理业务
	 */
	@Override
	protected void reduce(Text key, Iterable<trafficBean> values, Context context) 
			throws IOException, InterruptedException { 
		 
		long upSize_count = 0;
		long downSize_count = 0;
		
		for (trafficBean subBean : values) {
			upSize_count += subBean.getUpSize();
			downSize_count += subBean.getDownSize();
		}
		
		context.write(key,new trafficBean(key.toString(), upSize_count, downSize_count));
		
	}
	
}
