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
reduce ����:UserLocationBean		NullWritable		
reduce ���:UserLocationBean		NullWritable
 */
public class UserLocationReducer extends Reducer<UserLocationBean, NullWritable, UserLocationBean, NullWritable>{

	/**
	 * ������������key
	 * UserA,LocationA,2018-01-01 08:00:00,60
	 * UserA,LocationA,2018-01-01 09:00:00,60
	 * UserA,LocationB,2018-01-01 10:00:00,60
	 * UserA,LocationA,2018-01-01 11:00:00,60
	 */
	
	//reduce��key�Ķ���
	UserLocationBean outKey = new UserLocationBean();
	//���ڽ�����ʽ
	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	@Override
	protected void reduce(UserLocationBean key, Iterable<NullWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		//����ÿһ���������Ŀ�ֵvalue
		//�ж����ÿһ���key-value�ĵ�һ������Ϊһ��key��������key
		//�ж����ǵ�ʱ���Ƿ����������������Ϊͬһ�����ݣ�Ȼ��������key
		
		//ÿ�μ�¼���ϴμ�¼�Ƚϣ�������Ҫʵ�����������Ĺ��ܣ���Ҫ��дUserLocationBean��CompareTo����
		
		//����һ��������
		int count=0;
		for (NullWritable nullWritable : values) {
			count++;
			//�������һ��key-value�еĵ�һ��Ԫ��ʱ��ֱ�Ӹ�ֵ��outKey����
			if (count==1) {
				outKey.set(key);
			}else {
				//�����ж�ͣ��ʱ���Ƿ���������������ϲ�ͣ��ʱ��
				long firstTime=0;//��һ��ʱ��
				long lastTime=0;//���һ��ʱ��
				//
				try {
					//ÿ�α��������õ���ʱ���,ע�⣺Ϊ��ǰ����key
					firstTime=simpleDateFormat.parse(key.getTime()).getTime();
					//��һ����¼��ʱ�����ͣ��ʱ��֮��,ע�⣺�ϴεĶ���outKey
					//ʱ����ĵ�λΪ���룬��ͣ��ʱ��Ϊ�����赥λ����
					lastTime=simpleDateFormat.parse(outKey.getTime()).getTime()+outKey.getDuration()*60*1000; 
							} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// �����ȣ�֤����������¼�����Ժϲ�������outKey��ͣ��ʱ��
				if (firstTime == lastTime) {
					// �����ε�ͣ��ʱ���ϲ�������Ϊ�������ݵ�ͣ��ʱ��
					outKey.setDuration(outKey.getDuration() + key.getDuration());
				} else {
					// ��������
					// �������һ�����ݼ�¼
					context.write(outKey, nullWritable);

					// Ȼ���ٴμ�¼��ǰ����������һ����¼
					outKey.set(key);
				}
			}
		}
		
		//����������һ��
		context.write(outKey, NullWritable.get());//�����keyΪ����UserLocationBean ��valueΪ��
	}
}
