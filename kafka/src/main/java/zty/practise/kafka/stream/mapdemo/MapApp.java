package zty.practise.kafka.stream.mapdemo;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class MapApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//"Zhangty", 3L
		KStream<String, Long> simpleFirstStream = builder.stream("topicH", Consumed.with(Serdes.String(), Serdes.Long()));
		
		//map,一对一的转化
		KStream<String, String> mapStream = simpleFirstStream.map((key, value) -> KeyValue.pair(key.toLowerCase(), key.toUpperCase()));
		
		//mapValue，一对一转化，不涉及key
		KStream<String, String> mapValueStream = mapStream.mapValues(value -> "upper" + value);
		
		//peek,与foreach类似，无状态的操作，但不是终端操作不会中断流
		KStream<String, String> peekStream = mapValueStream.peek((key, value) -> System.out.println("peek:key=" + key +", value=" + value));
		
		//selectKey,修改key
		KStream<String, String> keyStream = peekStream.selectKey((key, value) -> value);
		
		keyStream.foreach((key, value) -> System.out.println("print:key=" + key + ",value=" +value));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
