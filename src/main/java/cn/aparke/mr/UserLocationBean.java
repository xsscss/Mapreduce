package cn.aparke.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//自己封装的对象类需要实现序列化和反序列化
//如需要将自定义的 bean 放在 key 中传输，则还需要实现 comparable 接口，
//因为 mapreduce 框中的 shuffle 过程一定会对 key 进行排序,此时，自定义的bean 实现的接口应该是：WritableComparable
public class UserLocationBean implements WritableComparable<UserLocationBean>{

	private String userId;
	private String locationId;
	private String time;
	private long duration;//持续时间
	
	//反序列化需要加载无参构造
	public UserLocationBean() {
		// TODO Auto-generated constructor stub
	}
	
	//切分数据设置每个属性
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
	
	//设置UserLocationBean的set方法
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
	 * hadoop系统在反序列化该类的对象时要调用的方法
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.userId = in.readUTF();
		this.locationId = in.readUTF();
		this.time = in.readUTF();
		this.duration = in.readLong();
	}
	/**
	 * hadoop系统在序列化该类的对象时要调用的方法
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(userId);
		out.writeUTF(locationId);
		out.writeUTF(time);
		out.writeLong(duration);
	}
	/**
	 * 排序规则
	 * 
	 * 按照 userId  locationId  和  time 排序  都是 升序
	 *  指定对象排序的方法
	 *  如果指定的数与参数相等返回 0。
	 *  如果指定的数小于参数返回 -1。
	 *  如果指定的数大于参数返回 1。
	 */
	
	public int compareTo(UserLocationBean o) {
		
		// 比较userid是否相等
		int comUserId = this.userId.compareTo(o.userId);
		if (comUserId == 0) {
			// 如果相等，比较locationId
			int comLocationId = this.locationId.compareTo(o.locationId);
			if (comLocationId == 0) {
				// 如果locationid也相等，再比较time
				int comTime = this.time.compareTo(o.time);
				if (comTime == 0) {
					// 如果time相等，则返回0
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
