package zty.practise.kafka.stream.mapdemo;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.KeyValue;

import com.google.common.collect.Lists;

public class FlatMapApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, Long> simpleFirstStream = builder.stream("topicB");
		
		//"Hello Zhangty", 3L
		KStream<String, Long> filterStream = simpleFirstStream.filterNot((key, value) -> value < 0);
		
		KStream<String, String> flatMapStream = simpleFirstStream.flatMap((key, value) -> {
			List<KeyValue<String, String>> result = Lists.newLinkedList();
			result.add(KeyValue.pair("upper", key.toUpperCase()));
			result.add(KeyValue.pair("lower", key.toLowerCase()));
			return result;
		});
		
		KStream<String, String> flatMapValueStream = flatMapStream.flatMapValues(value -> Arrays.asList(value.split("\\s+")));

		flatMapValueStream.to("topicA");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
