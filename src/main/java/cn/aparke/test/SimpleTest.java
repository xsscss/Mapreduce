package cn.aparke.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
/**
 * key���û�ID��λ��ID value����ʼʱ�䣬ͣ��ʱ�������ӣ�
 * ��û��ʵ������ʱ��ϲ��򵥵�wordcount,��������ĿҪ��
 *
	�������壺ĳ���û���ĳ��λ�ô�ĳ��ʱ�̿�ʼͣ���˶೤ʱ��
	�����߼���
	��ͬһ���û�����ͬһ��λ�ã������Ķ�����¼���кϲ�
	�ϲ�ԭ�򣺿�ʼʱ��ȡ����ģ�ͣ��ʱ���Ӻ�
	�û�ID��λ��ID����ʼʱ�䣬ͣ��ʱ�������ӣ�
 *	user_a,location_a,2018-01-01 08:00:00,60
 */
public class SimpleTest {
	 
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
//			user_a,location_a,2018-01-01 08:00:00,60
			String[] sp = value.toString().split(",");
//			String sb = sp[2].substring(11, 13);
			outkey.set(sp[0]+","+sp[1]);		//key���û�ID��λ��ID
			outvalue.set(sp[2]+","+sp[3]);		//value����ʼʱ�䣬ͣ��ʱ�������ӣ�
			context.write(outkey, outvalue);
		}
	}
	static class MyReducer extends Reducer<Text, Text, Text, Text>{
		List<String> list = new ArrayList<String>();
		Text outvalue = new Text();
		int sum=0;
		@Override
		protected void reduce(Text key,
				Iterable<Text> values, 
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
 
			//user_a,location_a,2018-01-01 08:00:00,60
			for(Text v:values){
				String[] sp = v.toString().split(",");
				sum+=Integer.parseInt(sp[1]);
//				System.out.println(sp[0]);
				list.add(sp[0]);		//��ӵ�list������
			}
			Collections.sort(list);		//��list��������2018-01-01 08:00:00�ַ���Ҳ�����ŵĴ���
			outvalue.set(list.get(0)+"\t"+sum);
			context.write(key, outvalue);
			sum=0;
			list.clear();		//Ϊ�˱�������һ�»���
		}
	}
 
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(SimpleTest.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
 

		// ָ����mapreduce�������ݵ���������·��
		Path inputPath = new Path("date/input");
		Path outputPath = new Path("date/outTest");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// ����ύ����
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);		
		
 
	}
 
}