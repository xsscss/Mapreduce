package cn.aparke.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author aparke
 ˼·������


�������location������ͬ����ô���ж�ʱ�䣨תΪlong�ͣ�����ÿ�������������У��жϵ�ǰʱ�����ͣ��ʱ���Ƿ���
��һ��������ȣ������Ϊһ�飬ͨ��mapreduce��һ��GroupingComparator��������ͬ�����key�ϲ�

id��Ϊkey�޷����ƣ� ������Ҫ�Զ����װ�������
��key����Ϊ value1��һ��ֵ  
 		  key       			value
map ����:Object					Text
map ���:UserLocationBean	NullWritable

�߼�����
1������userid��locationid����
2������userid��locationid��time����
 *
 */
public class UserLocationMapper extends Mapper<LongWritable, Text, UserLocationBean, NullWritable> {


	//ʵ����һ��UserLocationBean������Ϊ���keyֵ
	UserLocationBean outKey = new UserLocationBean();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		//����������UserA,LocationA,2018-01-01 08:00:00,60
		String[] split = value.toString().split(",");
		//��UserLocationBean�����ж���set(String [] split)�������Դ����������ֽ����и�
		outKey.set(split);
		//ͨ��key��UserLocation ���װ�õĴ���ȥ
		context.write(outKey, NullWritable.get());//value����ȥΪ��
	}
}
