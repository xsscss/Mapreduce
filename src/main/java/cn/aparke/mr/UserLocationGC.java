package cn.aparke.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserLocationGC extends WritableComparator{
/**
GroupingComparator是在reduce阶段分组来使用的，由于reduce阶段，如果key相同的一组，只取第一个key作为key，迭代所有的values。
 如果reduce的key是自定义的bean，我们只需要bean里面的某个属性相同就认为这样的key是相同的，
 这是我们就需要之定义GroupCoparator来“欺骗”reduce了。 我们需要理清楚的还有map阶段你的几个自定义：
 parttioner中的getPartition（）这个是map阶段自定义分区， bean中定义CopmareTo()是在溢出和merge时用来来排序的。 
 
 
 我们就需要使用GroupingComparatorClass来自定义分组方式。我们需要定义一个Comparator函数，令其继承WritableComparator，并重写compare方法。
 在compare方法方法中，我们定义规约器的key分组方式。
参考文章
https://www.cnblogs.com/jingpeng77/p/10098847.html
https://www.jianshu.com/p/b36d8c890e5c
 * 
 * 
 */
//
	public UserLocationGC() {
		super(UserLocationBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 强转 将WritableComparable转为UserLocationBean
		UserLocationBean bean_a = (UserLocationBean) a;
		UserLocationBean bean_b = (UserLocationBean) b;

		return bean_a.getUserId().compareTo(bean_b.getUserId());
	}
}
