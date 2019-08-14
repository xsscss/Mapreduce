package cn.aparke.mr;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserLocationJobToYarn {

	public static void main(String[] args) throws Exception {
		
		
		// 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
		System.setProperty("HADOOP_USER_NAME", "root");
		//加载配置参数
		Configuration conf = new Configuration();
		//设置文件系统为HDFS
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job = Job.getInstance(conf,"userLocation");
//		job.setJar("target/UserLocation-0.0.1-SNAPSHOT.jar");
		
		//设置提交的主类
		job.setJarByClass(UserLocationJobToYarn.class);
		//指定map reduce 类
		job.setMapperClass(UserLocationMapper.class);
		job.setReducerClass(UserLocationReducer.class);
		// 指定maptask的输出类型 可以省去
		job.setMapOutputKeyClass(UserLocationBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		// 指定job reducetask的输出类型
		job.setOutputKeyClass(UserLocationBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//如果连续**（注意，一定连续）**的两条或多条记录满足同组
		//（即compare方法返回0）的条件，那么会把这些key对应的value放在同一个集合中。
		job.setGroupingComparatorClass(UserLocationGC.class);
		
		
		Path inputPath = new Path("/myhdfs/userLocation/input");
		Path outputPath = new Path("/myhdfs/userLocation/output");
		
		FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		// 指定该mapreduce程序数据的输入和输出路径	
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// 提交
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

