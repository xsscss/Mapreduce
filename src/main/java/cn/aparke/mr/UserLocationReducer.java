package cn.aparke.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 
 * @author aparke
 * 		  key       				value
reduce 输入:UserLocationBean		NullWritable		
reduce 输出:UserLocationBean		NullWritable
 */
public class UserLocationReducer extends Reducer<UserLocationBean, NullWritable, UserLocationBean, NullWritable>{

	/**
	 * 传进来的数据key
	 * UserA,LocationA,2018-01-01 08:00:00,60
	 * UserA,LocationA,2018-01-01 09:00:00,60
	 * UserA,LocationB,2018-01-01 10:00:00,60
	 * UserA,LocationA,2018-01-01 11:00:00,60
	 */
	
	//reduce的key的对象
	UserLocationBean outKey = new UserLocationBean();
	//日期解析格式
	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	@Override
	protected void reduce(UserLocationBean key, Iterable<NullWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		//遍历每一条传过来的空值value
		//判断如果每一组的key-value的第一条都作为一个key，并设置key
		//判断他们的时间是否连续，如果连续则为同一组数据，然后再设置key
		
		//每次记录与上次记录比较，所以需要实现排序的升序的功能，需要重写UserLocationBean的CompareTo方法
		
		//定义一个计数器
		int count=0;
		for (NullWritable nullWritable : values) {
			count++;
			//如果是这一组key-value中的第一个元素时，直接赋值给outKey对象
			if (count==1) {
				outKey.set(key);
			}else {
				//否则判断停留时间是否连续，如果连续合并停留时间
				long firstTime=0;//第一个时间
				long lastTime=0;//最后一个时间
				//
				try {
					//每次遍历出来得到的时间戳,注意：为当前对象key
					firstTime=simpleDateFormat.parse(key.getTime()).getTime();
					//上一条记录的时间戳和停留时间之和,注意：上次的对象outKey
					//时间戳的单位为毫秒，而停留时长为分钟需单位换算
					lastTime=simpleDateFormat.parse(outKey.getTime()).getTime()+outKey.getDuration()*60*1000; 
							} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// 如果相等，证明是连续记录，所以合并，更新outKey的停留时长
				if (firstTime == lastTime) {
					// 将两次的停留时长合并，设置为上条数据的停留时长
					outKey.setDuration(outKey.getDuration() + key.getDuration());
				} else {
					// 如果不相等
					// 先输出上一条数据记录
					context.write(outKey, nullWritable);

					// 然后再次记录当前遍历到的这一条记录
					outKey.set(key);
				}
			}
		}
		
		//遍历完后，输出一次
		context.write(outKey, NullWritable.get());//输出的key为对象UserLocationBean ，value为空
	}
}
