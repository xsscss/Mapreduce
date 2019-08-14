package cn.aparke.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 传进来的数据key
 * user_a	location_a	2018-01-01 08:00:00	60
 * user_a	location_a	2018-01-01 09:00:00	60
 * user_a	location_a	2018-01-01 11:00:00	60
 * user_a	location_a	2018-01-01 12:00:00	60
 */
public class SimpleDateFormatUtilTest {
	
	public static void main(String[] args) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String date1="2018-01-01 08:00:00";
		String date2="2018-01-01 10:00:00";
		//停留时长
		int duration=60;
		
		//将时间解析为long值 为时间戳 单位为毫秒
		long parse1 = simpleDateFormat.parse(date1).getTime();
		long parse2 = simpleDateFormat.parse(date2).getTime();
		System.out.println(parse1);
		System.out.println(parse2);
		
		//下一次时间 等于上一次时间时间戳+停留时间duration（分钟）
		long sum=parse1+duration*60*1000;
		
		//判断 是否相等
		System.out.println(parse2==sum);
		
	}

	
}
