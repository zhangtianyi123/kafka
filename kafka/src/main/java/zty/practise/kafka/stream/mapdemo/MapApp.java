package zty.practise.kafka.stream.mapdemo;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class MapApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//"Zhangty", 3L
		KStream<String, Long> simpleFirstStream = builder.stream("topicB");
		
		//map
		KStream<String, String> mapStream = simpleFirstStream.map((key, value) -> KeyValue.pair(key.toLowerCase(), key.toUpperCase()));
		
		//mapValue
		KStream<String, String> mapValueStream = mapStream.mapValues(value -> "upper" + value);
		
		//peek
		KStream<String, String> peekStream = mapValueStream.peek((key, value) -> System.out.println("key=" + key +", value=" + value));
		
		//selectKey
		KStream<String, String> keyStream = peekStream.selectKey((key, value) -> value);
		
		keyStream.to("topicB");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
