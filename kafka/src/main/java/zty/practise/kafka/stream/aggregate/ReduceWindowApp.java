package zty.practise.kafka.stream.aggregate;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * 基于时间窗口的reduce操作
 * 
 * 实现单词计数的功能
 * 
 * @author zhangtianyi
 *
 */
public class ReduceWindowApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reducewindow_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//源topic
		KStream<String, String> textLines = builder.stream("topicA", Consumed.with(Serdes.String(), Serdes.String()));

		//无状态的分组操作转为KGroupedStream
		KStream<String, String> wordSplitStream = textLines
	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")));
		
		KGroupedStream<String, Long> wordGroupedStream = wordSplitStream
				//map转换key,value值和类型，因为reduce无法更改值类型
				.map((key, value) -> KeyValue.pair(value, 1L))
	            //由于不修改key,使用groupByKey 替换 groupBy
	            .groupByKey();
		
		//KGroupedStream执行聚合转为KTable
		//reduce 没有初始化器，也不能转换值
		KTable<Windowed<String>, Long> wordAggregatedStream = wordGroupedStream
				.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
				.reduce((aggValue, newValue) -> aggValue + newValue,
			    Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("reduce-timewindow--aggregated-store")
			    .withValueSerde(Serdes.Long())); 
		
		//KTable -> KStream
		wordAggregatedStream.toStream()
		.map((k, v) -> new KeyValue<>(k.key(), v))
		.to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
