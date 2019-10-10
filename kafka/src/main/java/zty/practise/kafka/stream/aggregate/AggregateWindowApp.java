package zty.practise.kafka.stream.aggregate;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

/**
 * 聚合可以理解为reduce等的一般化
 * 
 * 通过Aggregate(By Window) 实现单词计数
 * 
 * 基于每个窗口对记录值进行聚合
 *
 * 仅仅针对KGroupedStream → KTable
 *
 * 仍然需要提供初始化器和加法器，以及一个窗口
 * 当窗口基于会话的时候，还需要额外提供一个会话合并聚合器
 *
 * 窗口流需要考虑Time情况和Session情况
 *
 * @author zhangtianyi
 *
 */
public class AggregateWindowApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregatewindow_app_id");
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
		KTable<Windowed<String>, Long> wordTimeWindowAggregatedStream = wordGroupedStream
				.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
				.aggregate(
			    () -> 0L, 
			    (aggKey, newValue, aggValue) -> aggValue + 1L,
			    Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-store")
			    .withValueSerde(Serdes.Long())); 
		
		//KTable -> KStream
		wordTimeWindowAggregatedStream.toStream()
		.map((k, v) -> new KeyValue<>(k.key(), v))
		.to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
