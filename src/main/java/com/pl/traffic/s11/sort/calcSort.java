package com.pl.traffic.s11.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pl.comm.dfsConfig;
import com.pl.traffic.s1.sum.trafficBean;

/**
 * 对手机号的流量合计 进行排序
 * 
 * @author max400
 *
 */
public class calcSort {

	public static class SortMapper extends Mapper<LongWritable, Text, trafficBean, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 拆分行数据
			String line = value.toString();
			String[] fields = line.split("\t");

			// 解析业务数据
			String phone = fields[0];
			long upSize = Long.parseLong(fields[1]);
			long downSize = Long.parseLong(fields[2]);

			// 发送KV数据流
			context.write(new trafficBean(phone, upSize, downSize), NullWritable.get());
		}
	}

	public static class SortReducer extends Reducer<trafficBean, NullWritable, Text, trafficBean> {

		@Override
		protected void reduce(trafficBean key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			String phone = key.getPhone();
			context.write(new Text(phone), key);
		}
	}

	// 本次计算的输入上com.pl.traffic.s1.sum.calcRunner 的计算结果
	static String dfs_path_in = dfsConfig.DFS_PATH_OUT + "/sum";
	static String dfs_path_out = dfsConfig.DFS_PATH_OUT + "/sort";
	static String dfs_full_path_in = dfsConfig.DFS_FULL_PATH_OUT + "/sum";
	static String dfs_full_path_out = dfsConfig.DFS_FULL_PATH_OUT + "/sort";

	public static void main(String[] args) throws Exception {

		System.out.println(dfsConfig.LOG_FLAG + dfs_path_in);
		System.out.println(dfsConfig.LOG_FLAG + dfs_path_out);
		System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_in);
		System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_out);

		// 配置服务器
		Configuration conf = dfsConfig.getConf();
		// 删除历史输出目录
		FileSystem fileSystem = FileSystem.get(conf);
		boolean reuslt = fileSystem.exists(new Path(dfs_path_out));
		if (reuslt) {
			reuslt = fileSystem.delete(new Path(dfs_path_out), true);
			System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_out + "   delete..." + reuslt);
		}

		// ------------------------
		Job job = Job.getInstance(conf);
		job.setJarByClass(calcSort.class);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(trafficBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(trafficBean.class);

		FileInputFormat.setInputPaths(job, new Path(dfs_full_path_in));
		FileOutputFormat.setOutputPath(job, new Path(dfs_full_path_out));

		int res = job.waitForCompletion(true) ? 0 : 1;
		String resStr = (res == 0) ? "OK!" : "NG!";
		System.out.println(dfsConfig.LOG_FLAG + " output: " + dfs_full_path_out + " ..." + resStr);

	}
}
