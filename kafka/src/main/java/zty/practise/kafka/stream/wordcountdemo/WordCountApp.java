package zty.practise.kafka.stream.wordcountdemo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
 
import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//源topic
		KStream<String, String> textLines = builder.stream("topicA");

		KTable<String, Long> wordCounts = textLines
	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
	            .groupBy((key, word) -> word)
	            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
		wordCounts.toStream().to("topicD", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
