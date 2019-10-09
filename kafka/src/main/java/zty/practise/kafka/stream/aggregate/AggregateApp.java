package zty.practise.kafka.stream.aggregate;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * 聚合可以理解为reduce等的一般化
 * 
 * 通过Aggregate 实现单词计数
 * 
 * 在分组之后，可以通过**aggregate()/count()/reduce()**等有状态操作 
 * 将KGroupedStream和KGroupedTable转回KStream和KTable
 * 
 * GroupByKey 根据已有键key分组
 * 将KStream → KGroupedStream 
 *
 * GroupBy（selectKey + groupByKey）  先设置键key 再分组
 * 将KStream → KGroupedStream / KTable → KGroupedTable
 *
 * @author zhangtianyi
 *
 */
public class AggregateApp {

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

		//无状态的分组操作转为KGroupedStream
		KGroupedStream<String, String> wordGroupedStream = textLines
	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
	            .groupBy((key, word) -> word);
		
		//KGroupedStream执行聚合转为KTable
		KTable<String, Long> wordAggregatedStream = wordGroupedStream.aggregate(
			    () -> 0L, 
			    (aggKey, newValue, aggValue) -> aggValue + 1L,
			    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store")
			    .withValueSerde(Serdes.Long())); 
		
		//KTable -> KStream
		wordAggregatedStream.toStream().to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
