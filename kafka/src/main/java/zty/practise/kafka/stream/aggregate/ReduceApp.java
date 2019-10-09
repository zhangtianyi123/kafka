package zty.practise.kafka.stream.aggregate;

import java.util.Arrays;
import java.util.List;
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
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * 滚动的聚合，通过分组的key组合记录的值，将每一个当前值与上一个reduce的值合并
 * 
 * 并返回新的reduce值，与聚合不同的是，结果值类型无法更改。
 * reduce分组流的时候，必须提供加法器，当reduce分组表的时候，还需要再提供减法器，原理与聚合操作时相同
 * 
 * @author zhangtianyi
 *
 */
public class ReduceApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount_app_id");
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
		KTable<String, Long> wordAggregatedStream = wordGroupedStream.reduce(
			    (aggValue, newValue) -> aggValue + newValue,
			    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-aggregated-store")
			    .withValueSerde(Serdes.Long())); 
		
		//KTable -> KStream
		wordAggregatedStream.toStream().to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
