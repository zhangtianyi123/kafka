package zty.practise.kafka.stream.upperdemo;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import com.google.common.collect.Lists;

public class StreamUpperAndBranchApp {

	/**
	 * 处理1：小写转大写
	 * 处理2：根据含有B/C字符条件分支到不同的流
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> simpleFirstStream = builder.stream("topicA");

		KStream<String, String> upperStream = simpleFirstStream.mapValues(s -> s.toUpperCase());
		Predicate<String, String> predicateB = (key, value) -> value.contains("B");
		Predicate<String, String> predicateC = (key, value) -> value.contains("C");
		
		KStream<String, String>[] branchs = upperStream.branch(predicateB, predicateC);
		branchs[0].to("topicB");
		branchs[1].to("topicC");
		

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
