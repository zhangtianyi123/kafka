package zty.practise.kafka.stream.upperdemo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

/**
 * foreach是终端操作和to不兼容
 * 比如打印，比如输出到内存缓存nosql/保存到db等等
 * @author zhangtianyi
 *
 */
public class StreamUpperForeachApp {
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamupperforeach_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> simpleFirstStream = builder.stream("topicA");

		ForeachAction<String, String> purchaseForeachAction = (key, value) -> System.out.println("@key=" + key);
		
		
		KStream<String, String> upperStream = simpleFirstStream.mapValues(s -> s.toUpperCase());
		upperStream.filter((key, value) -> value.contains("B"))
		.foreach(purchaseForeachAction);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
