package cn.aparke.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//�Լ���װ�Ķ�������Ҫʵ�����л��ͷ����л�
//����Ҫ���Զ���� bean ���� key �д��䣬����Ҫʵ�� comparable �ӿڣ�
//��Ϊ mapreduce ���е� shuffle ����һ����� key ��������,��ʱ���Զ����bean ʵ�ֵĽӿ�Ӧ���ǣ�WritableComparable
public class UserLocationBean implements WritableComparable<UserLocationBean>{

	private String userId;
	private String locationId;
	private String time;
	private long duration;//����ʱ��
	
	//�����л���Ҫ�����޲ι���
	public UserLocationBean() {
		// TODO Auto-generated constructor stub
	}
	
	//�з���������ÿ������
	public void set(String[] split) {
//		this.userId=split[0];
//		this.locationId=split[1];
//		this.time=split[2];
//		this.duration=Long.parseLong(split[3]);
		
		this.setUserId(split[0]);
		this.setLocationId(split[1]);
		this.setTime(split[2]);
		this.setDuration(Long.parseLong(split[3]));
	}
	
	//����UserLocationBean��set����
	public void set(UserLocationBean userLocationBean){
		this.setUserId(userLocationBean.getUserId());
		this.setLocationId(userLocationBean.getLocationId());
		this.setTime(userLocationBean.getTime());
		this.setDuration(userLocationBean.getDuration());
	}
	
	@Override
	public String toString() {
//		return userId + "\t" + locationId + "\t" + time + "\t" + duration;
		return userId + "," + locationId + "," + time + "," + duration;
		
	}




	public UserLocationBean(String userId, String locationId, String time, long duration) {
		super();
		this.userId = userId;
		this.locationId = locationId;
		this.time = time;
		this.duration = duration;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}


	public String getLocationId() {
		return locationId;
	}

	public void setLocationId(String locationId) {
		this.locationId = locationId;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}
	/**
	 * hadoopϵͳ�ڷ����л�����Ķ���ʱҪ���õķ���
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.userId = in.readUTF();
		this.locationId = in.readUTF();
		this.time = in.readUTF();
		this.duration = in.readLong();
	}
	/**
	 * hadoopϵͳ�����л�����Ķ���ʱҪ���õķ���
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(userId);
		out.writeUTF(locationId);
		out.writeUTF(time);
		out.writeLong(duration);
	}
	/**
	 * �������
	 * 
	 * ���� userId  locationId  ��  time ����  ���� ����
	 *  ָ����������ķ���
	 *  ���ָ�������������ȷ��� 0��
	 *  ���ָ������С�ڲ������� -1��
	 *  ���ָ���������ڲ������� 1��
	 */
	
	public int compareTo(UserLocationBean o) {
		
		// �Ƚ�userid�Ƿ����
		int comUserId = this.userId.compareTo(o.userId);
		if (comUserId == 0) {
			// �����ȣ��Ƚ�locationId
			int comLocationId = this.locationId.compareTo(o.locationId);
			if (comLocationId == 0) {
				// ���locationidҲ��ȣ��ٱȽ�time
				int comTime = this.time.compareTo(o.time);
				if (comTime == 0) {
					// ���time��ȣ��򷵻�0
					return 0;
				} else {
					return comTime > 0 ? 1 : -1;
				}
			} else {
				return comLocationId > 0 ? 1 : -1;
			}
		} else {
			return comUserId > 0 ? 1 : -1;
		}
	}
}
