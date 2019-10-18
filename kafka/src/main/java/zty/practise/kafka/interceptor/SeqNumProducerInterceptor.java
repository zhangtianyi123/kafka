package zty.practise.kafka.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 序号拦截器，在消息发送前为消息增加序号
 * @author zhangtianyi
 */
public class SeqNumProducerInterceptor implements ProducerInterceptor {

	static int seqNum = 0;
	
	@Override
	public void configure(Map<String, ?> configs) {
	}

	/**
	 * 方法封装进KafkaProducer.send方法中，即它运行在用户主线程中的。
	 * Producer确保在消息被序列化以计算分区前调用该方法。
	 * 可以在该方法中对消息做任何操作，但最好保证不要修改消息所属的topic和分区，否则会影响目标分区的计算
	 */
	@Override
	public ProducerRecord onSend(ProducerRecord record) {
		 return new ProducerRecord(record.topic(), record.partition(), record.timestamp(), String.valueOf(seqNum++), record.value());
	}

	/**
	 * onAcknowledgement运行在producer的IO线程中，
	 * 因此不要在该方法中放入很重的逻辑，否则会拖慢producer的消息发送效率
	 */
	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
	}

	@Override
	public void close() {
	}

}
