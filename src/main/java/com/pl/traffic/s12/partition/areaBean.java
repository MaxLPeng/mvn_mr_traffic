package com.pl.traffic.s12.partition;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

import com.pl.traffic.s1.sum.trafficBean;

/**
 * 分组实现
 * @author max400
 *
 * @param <KEY>
 * @param <VALUE>
 */ 
public class areaBean<KEY, VALUE> extends Partitioner<KEY, VALUE> {

	private  static HashMap<String,Integer> areaMap = new HashMap<String, Integer>();
	
	static {
		areaMap.put("130", 0);
		areaMap.put("131", 1);
		areaMap.put("132", 2);
		areaMap.put("133", 3); 
	} 

	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {
		
		trafficBean traffic = new trafficBean();
		traffic = (trafficBean) key;
		Integer partCode = areaMap.get(traffic.getPhone().substring(0, 3));
		
		// TODO Auto-generated method stub
		return (partCode!=null)?partCode:9;
	}

}
