package zty.practise.kafka.stream.aggregate;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

import com.google.common.collect.Maps;

/**
 * zty.practise.kafka.stream.aggregate包下的转换均是基于KStream的
 * 
 * AggregateTableApp通过NBA球员转会的例子来展示对于KTable的聚合操作
 * 
 * @author zhangtianyi
 *
 */
public class AggregateTableApp {
	
	private static Map<String, Long> playerAndSalary  = Maps.newHashMap();
	
	static {
		playerAndSalary.put("james", 5000L);
		playerAndSalary.put("kobe", 4000L);
		playerAndSalary.put("davis", 3000L);
	}
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregatetable_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//以表的形式接收流数据  eg:("james", "laker") 
		KTable<String, String> playerAndTeams = builder.table("topicA");
		
		//按照球队分组球员，value关注球员的工资薪水
		KGroupedTable<String, Long> groupedTable = playerAndTeams
				.groupBy((player, team) -> KeyValue.pair(team, playerAndSalary.get(player)), Serialized.with(
					      Serdes.String(), 
					      Serdes.Long()));
		
		//KGroupedStream执行聚合转为KTable
		KTable<String, Long> teamAggregatedTable = groupedTable.aggregate(
			    () -> 0L, 
			    (aggKey, newValue, aggValue) -> aggValue + newValue,
			    (aggKey, oldValue, aggValue) -> aggValue - oldValue,
			    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-table-store")
			    .withKeySerde(Serdes.String())
			    .withValueSerde(Serdes.Long())); 
		
		//KTable -> KStream
		teamAggregatedTable.toStream().to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}

}
