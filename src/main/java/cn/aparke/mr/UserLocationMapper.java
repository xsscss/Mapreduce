package cn.aparke.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author aparke
 思路分析：


如果参数location参数相同，那么再判断时间（转为long型），对每条数据升序排列，判断当前时间加上停留时间是否与
下一条数据相等，相等则划为一组，通过mapreduce的一个GroupingComparator类来将相同的组的key合并

id作为key无法控制， 所以需要自定义封装类来完成
将key设置为 value1第一个值  
 		  key       			value
map 输入:Object					Text
map 输出:UserLocationBean	NullWritable

逻辑处理：
1、按照userid和locationid分组
2、按照userid和locationid和time排序
 *
 */
public class UserLocationMapper extends Mapper<LongWritable, Text, UserLocationBean, NullWritable> {


	//实例化一个UserLocationBean对象作为输出key值
	UserLocationBean outKey = new UserLocationBean();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		//数据样例：UserA,LocationA,2018-01-01 08:00:00,60
		String[] split = value.toString().split(",");
		//在UserLocationBean对象中定义set(String [] split)方法，对传进来的数字进行切割
		outKey.set(split);
		//通过key将UserLocation 类封装好的传过去
		context.write(outKey, NullWritable.get());//value传过去为空
	}
}
