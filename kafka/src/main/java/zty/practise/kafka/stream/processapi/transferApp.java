package zty.practise.kafka.stream.processapi;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class transferApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transfer_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputStream = builder.stream("topicA");

		// 创建状态存储
		StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder = Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore("myTransformState"), Serdes.String(), Serdes.String());

		//注册状态存储
		builder.addStateStore(keyValueStoreBuilder);
		 
		//流执行自定义的transform
		KStream<String, String> outputStream = inputStream.transform(new TransformerSupplier<String, String, KeyValue<String, String>>() {
		     public Transformer<String, String, KeyValue<String, String>> get() {
		         return new MyTransformer();
		        }
		 }, "myTransformState");
		
		outputStream.to("topicD", Produced.with(Serdes.String(), Serdes.String()));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}

	static class MyTransformer implements Transformer<String, String, KeyValue<String, String>> {

		private ProcessorContext context;
		private StateStore state;

		@Override
		public void init(ProcessorContext context) {
			this.context = context;
			this.state = context.getStateStore("myTransformState");
			context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> System.out.println());
		}

		@Override
		public KeyValue<String, String> transform(String key, String value) {
			return new KeyValue<String, String>("lower=" + key.toLowerCase(), "upper=" + value);
		}

		@Override
		public void close() {
		}

	}
}
