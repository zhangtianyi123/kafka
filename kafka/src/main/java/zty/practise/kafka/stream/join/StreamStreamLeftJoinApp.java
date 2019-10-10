package zty.practise.kafka.stream.join;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class StreamStreamLeftJoinApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamstreamLeftjoin_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//源topic
		KStream<String, Long> leftStream = builder.stream("topicLong", Consumed.with(Serdes.String(), Serdes.Long()));
		KStream<String, Double> rightStream = builder.stream("topicDouble", Consumed.with(Serdes.String(), Serdes.Double()));
		
		KStream<String, String> joinedStream = leftStream.leftJoin(rightStream,
			    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
			    JoinWindows.of(TimeUnit.MINUTES.toMillis(2)),
			    Joined.with(
			      Serdes.String(), /* key */
			      Serdes.Long(),   /* left value */
			      Serdes.Double())  /* right value */
			  );
		
		joinedStream.peek((key, value) -> System.out.println("joinedStream:key=" + key + ", value=" + value));
		joinedStream.to("topicA", Produced.with(Serdes.String(), Serdes.String()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
