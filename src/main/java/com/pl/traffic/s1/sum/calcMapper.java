package com.pl.traffic.s1.sum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
  
public class calcMapper extends Mapper<LongWritable, Text, Text, trafficBean>{
	
	//拿到日志中每一行的数据
	//封装成kv发送
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//拆分行数据
		String line = value.toString();
		String[] fields = line.split("\t");
		
		//解析业务数据 
		String phone = fields[0];
		long upSize = Long.parseLong(fields[2]) ;
		long downSize = Long.parseLong(fields[3]) ;
		
		//发送KV数据流
		context.write(new Text(phone), new trafficBean(phone, upSize, downSize));
		
	}

}
