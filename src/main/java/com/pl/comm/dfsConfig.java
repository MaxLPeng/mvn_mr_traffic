package com.pl.comm;

import org.apache.hadoop.conf.Configuration;

public class dfsConfig {

	public final static String LOG_FLAG 		= "*** Calc Traffic :";
	public final static String LOCAL_PATH 		= "e:/tmp";
	public final static String LOCAL_FILE 		= "/Traffic.data";
	public final static String FS_DEFAULTFS		= "hdfs://namecluster1";
	public final static String DFS_PATH_ROOT	= "/test";
	public final static String DFS_PATH_IN 		= DFS_PATH_ROOT + "/in";
	public final static String DFS_FULL_PATH_IN = FS_DEFAULTFS + DFS_PATH_ROOT + "/in"; 
	public final static String DFS_PATH_OUT		= DFS_PATH_ROOT + "/out"; 
	public final static String DFS_FULL_PATH_OUT= FS_DEFAULTFS + dfsConfig.DFS_PATH_ROOT + "/out";
	
	public final static String DFS_FILE_IN 		= "/Traffic.data";
	public final static String DFS_JAR 			= "target/mvn_mr_traffic-0.0.1-SNAPSHOT.jar";

	// 配置服务器
	static Configuration _conf = null;

	public static Configuration getConf() {

		if (_conf == null) {
			// 配置服务器
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", dfsConfig.FS_DEFAULTFS);
			conf.set("dfs.replication", "2");
			// HDFS-HA
			conf.set("dfs.nameservices", "namecluster1");
			conf.set("dfs.ha.namenodes.namecluster1", "nn1,nn2");
			conf.set("dfs.namenode.rpc-address.namecluster1.nn1", "NameNode01:9000");
			conf.set("dfs.namenode.rpc-address.namecluster1.nn2", "NameNode02:9000");
			conf.set("dfs.client.failover.proxy.provider.namecluster1",
					"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

			conf.set("mapreduce.framework.name", "yarn");
			conf.set("mapreduce.app-submission.cross-platform", "true");
			// RM非HA
			// conf.set("yarn.resourcemanager.hostname", YARN_RM_HOSTNAME);
			conf.set("mapreduce.job.jar", dfsConfig.DFS_JAR);
			// RM-HA
			conf.set("yarn.resourcemanager.ha.enabled", "true");
			conf.set("yarn.resourcemanager.cluster-id", "yarncluster");
			conf.set("yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
			conf.set("yarn.resourcemanager.hostname.rm1", "DataNode01");
			conf.set("yarn.resourcemanager.hostname.rm2", "DataNode02"); 
			 
			_conf = conf;
		}

		return _conf;
	}

}
