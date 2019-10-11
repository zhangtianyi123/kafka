package zty.practise.kafka.stream.join;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class KStreamGlobalKTableLeftJoinApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamglobaltableleftjoin_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, Long> leftStream = builder.stream("topicLong", Consumed.with(Serdes.String(), Serdes.Long()));
		GlobalKTable<String, Double> rightGlobalTable = builder.globalTable("topicDouble", Consumed.with(Serdes.String(), Serdes.Double()));
		
		/**
		 * 通过变更的key来执行查找，查找比原key多一个#的表数据
		 */
		KStream<String, String> joinedStream = leftStream.leftJoin(rightGlobalTable,
				 (leftKey, leftValue) -> leftKey + "#",
			    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue);
		
		joinedStream.to("topicA", Produced.with(Serdes.String(), Serdes.String()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
	}
}
