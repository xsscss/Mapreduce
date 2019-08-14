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
 * key发用户ID，位置ID value发开始时间，停留时长（分钟）
 * 并没有实现连续时间合并简单的wordcount,不符合题目要求
 *
	数据意义：某个用户在某个位置从某个时刻开始停留了多长时间
	处理逻辑：
	对同一个用户，在同一个位置，连续的多条记录进行合并
	合并原则：开始时间取最早的，停留时长加和
	用户ID，位置ID，开始时间，停留时长（分钟）
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
			outkey.set(sp[0]+","+sp[1]);		//key发用户ID，位置ID
			outvalue.set(sp[2]+","+sp[3]);		//value发开始时间，停留时长（分钟）
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
				list.add(sp[0]);		//添加到list集合中
			}
			Collections.sort(list);		//对list集合排序（2018-01-01 08:00:00字符串也可以排的处理）
			outvalue.set(list.get(0)+"\t"+sum);
			context.write(key, outvalue);
			sum=0;
			list.clear();		//为了保险清理一下缓存
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
 

		// 指定该mapreduce程序数据的输入和输出路径
		Path inputPath = new Path("date/input");
		Path outputPath = new Path("date/outTest");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// 最后提交任务
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0 : 1);		
		
 
	}
 
}