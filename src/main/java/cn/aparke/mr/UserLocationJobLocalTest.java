package cn.aparke.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserLocationJobLocalTest {

	public static void main(String[] args) throws Exception {
		//�������ò���
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//�����ύ������
		job.setJarByClass(UserLocationJobLocalTest.class);
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
		
		Path inputPath = new Path("data/input");
		Path outputPath = new Path("data/output");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
	}
		// ָ����mapreduce�������ݵ���������·��	
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// �ύ
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

