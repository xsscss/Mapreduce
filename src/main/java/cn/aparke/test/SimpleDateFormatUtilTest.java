package cn.aparke.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ������������key
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
		//ͣ��ʱ��
		int duration=60;
		
		//��ʱ�����Ϊlongֵ Ϊʱ��� ��λΪ����
		long parse1 = simpleDateFormat.parse(date1).getTime();
		long parse2 = simpleDateFormat.parse(date2).getTime();
		System.out.println(parse1);
		System.out.println(parse2);
		
		//��һ��ʱ�� ������һ��ʱ��ʱ���+ͣ��ʱ��duration�����ӣ�
		long sum=parse1+duration*60*1000;
		
		//�ж� �Ƿ����
		System.out.println(parse2==sum);
		
	}

	
}
