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
		
		
		// �ڴ���������JVMϵͳ���������ڸ�job��������ȡ����HDFS���û����
		System.setProperty("HADOOP_USER_NAME", "root");
		//�������ò���
		Configuration conf = new Configuration();
		//�����ļ�ϵͳΪHDFS
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job = Job.getInstance(conf,"userLocation");
//		job.setJar("target/UserLocation-0.0.1-SNAPSHOT.jar");
		
		//�����ύ������
		job.setJarByClass(UserLocationJobToYarn.class);
		//ָ��map reduce ��
		job.setMapperClass(UserLocationMapper.class);
		job.setReducerClass(UserLocationReducer.class);
		// ָ��maptask��������� ����ʡȥ
		job.setMapOutputKeyClass(UserLocationBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		// ָ��job reducetask���������
		job.setOutputKeyClass(UserLocationBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//�������**��ע�⣬һ��������**�������������¼����ͬ��
		//����compare��������0������������ô�����Щkey��Ӧ��value����ͬһ�������С�
		job.setGroupingComparatorClass(UserLocationGC.class);
		
		
		Path inputPath = new Path("/myhdfs/userLocation/input");
		Path outputPath = new Path("/myhdfs/userLocation/output");
		
		FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		// ָ����mapreduce�������ݵ���������·��	
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// �ύ
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

