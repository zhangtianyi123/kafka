package zty.practise.kafka.interceptor;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * 消费者端的拦截器
 * 在生产者端的拦截器中已经
 * @author zhangtianyi
 *
 */
public class SeqNumConsumerInterceptor implements ConsumerInterceptor<String, String> {

	static int preNum = 0;
	
	@Override
	public void configure(Map<String, ?> configs) {
	}

	/**
	 * 消费者在正式处理消息之前调用
	 * 对（一批）消息处理
	 */
	@Override
	public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
		for(ConsumerRecord<String, String> record : records) {
			checkOrder(record.key());
		}
		
		return records;
	}
	
	/**
	 * 检查消息的顺序性
	 */
	private void checkOrder(String key) {
		if(StringUtils.equals("0", key) || StringUtils.equals(String.valueOf(++preNum), key)) {
			System.out.println(key + "顺序正确");
		} else {
			System.out.println(key + "顺序错误");
		}
	}

	@Override
	public void onCommit(Map offsets) {
	}

	@Override
	public void close() {
	}
	
}
