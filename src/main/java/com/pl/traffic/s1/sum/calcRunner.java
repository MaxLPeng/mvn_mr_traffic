package com.pl.traffic.s1.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.pl.comm.dfsConfig;

/**
 * 对手机流量进行合计操作
 * 
 * @author max400
 *
 */
public class calcRunner extends Configured implements Tool {

	static String dfs_path_in = dfsConfig.DFS_PATH_IN + "/traffic";
	static String dfs_path_out = dfsConfig.DFS_PATH_OUT + "/sum";
	static String dfs_full_path_out = dfsConfig.DFS_FULL_PATH_OUT + "/sum";

	public int run(String[] arg0) throws Exception {

		System.out.println(dfsConfig.LOG_FLAG + dfs_path_in);
		System.out.println(dfsConfig.LOG_FLAG + dfs_path_out);
		System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_out);

		// 配置服务器
		Configuration conf = dfsConfig.getConf();
		// 删除历史输出目录
		FileSystem fileSystem = FileSystem.get(conf);
		System.out.println(dfsConfig.LOG_FLAG + dfs_path_out);
		boolean reuslt = fileSystem.exists(new Path(dfs_path_out));
		if (reuslt) {
			reuslt = fileSystem.delete(new Path(dfs_path_out), true);
			System.out.println(dfsConfig.LOG_FLAG + dfs_path_out + "   delete..." + reuslt);
		}

		// ------------------------
		Job job = Job.getInstance(conf);
		job.setJarByClass(calcRunner.class);

		job.setMapperClass(calcMapper.class);
		job.setReducerClass(calcReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(trafficBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(trafficBean.class);

		FileInputFormat.setInputPaths(job, new Path(dfs_path_in));
		FileOutputFormat.setOutputPath(job, new Path(dfs_full_path_out));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new calcRunner(), args);
		String resStr = (res == 0) ? "OK!" : "NG!";
		System.out.println(dfsConfig.LOG_FLAG + " output: " + dfs_full_path_out + " ..." + resStr);
	}

}
