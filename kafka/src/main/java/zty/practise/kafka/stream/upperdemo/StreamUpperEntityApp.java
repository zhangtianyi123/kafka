package zty.practise.kafka.stream.upperdemo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import zty.practise.kafka.model.RequestEntity;
import zty.practise.kafka.stream.serde.SerdeUtil;

/**
 * 获取实体中的属性lotName（字符串）
 * @author zhangtianyi
 *
 */
public class StreamUpperEntityApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Serde<String> stringSerde = Serdes.String();
        Serde<RequestEntity> requestEntitySerde = SerdeUtil.RequestEntitySerde();
        
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, RequestEntity> simpleFirstStream = builder.stream("topicA", Consumed.with(stringSerde, requestEntitySerde));

		
		KStream<String, String> upperStream = simpleFirstStream.mapValues( requestEntity -> requestEntity.getLotName());
		upperStream.to("topicD");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
