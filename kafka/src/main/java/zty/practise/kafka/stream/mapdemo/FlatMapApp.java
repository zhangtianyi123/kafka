package zty.practise.kafka.stream.mapdemo;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.KeyValue;

import com.google.common.collect.Lists;

public class FlatMapApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "flatmap_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//比如："Hello Zhangty", 3L
		//注意明确指定Serde 否则会默认启用配置
		KStream<String, Long> simpleFirstStream = builder.stream("topicH", Consumed.with(Serdes.String(), Serdes.Long()));
		
		//反向过滤,黑名单，布尔型
		KStream<String, Long> filterStream = simpleFirstStream.filterNot((key, value) -> value < 0);
		
		//一对N的映射，key,value包括类型均可修改，返回键值对列表
		KStream<String, String> flatMapStream = simpleFirstStream.flatMap((key, value) -> {
			List<KeyValue<String, String>> result = Lists.newLinkedList();
			result.add(KeyValue.pair("upper", key.toUpperCase()));
			result.add(KeyValue.pair("lower", key.toLowerCase()));
			return result;
		});
		
		//一对N的映射，value包括类型均可修改，key不能修改，故而不会改变数据分区，返回value的列表（类似ETL中行转列的操作）
		KStream<String, String> flatMapValueStream = flatMapStream.flatMapValues(value -> Arrays.asList(value.split("\\s+")));

		flatMapValueStream.foreach((key, value) -> System.out.println("key=" + key + ",value=" +value));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
