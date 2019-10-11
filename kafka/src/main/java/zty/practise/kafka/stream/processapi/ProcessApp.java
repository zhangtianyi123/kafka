package zty.practise.kafka.stream.processapi;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.Processor;

public class ProcessApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "process_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputStream = builder.stream("topicA");

		// 创建状态存储
		StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder = Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore("myProcessorState"), Serdes.String(), Serdes.String());

		//注册状态存储
		builder.addStateStore(keyValueStoreBuilder);
		 
		//流执行自定义的process
		inputStream.process(new ProcessorSupplier<String, String>() {
		     public Processor<String, String> get() {
		         return new MyProcess();
		        }
		 }, "myProcessorState");
		
		KStream outputStream = inputStream.mapValues(s -> s.toUpperCase());
		
		outputStream.to("topicD", Produced.with(Serdes.String(), Serdes.String()));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
	
	static class MyProcess implements Processor<String, String> {

		private StateStore state;
		
		@Override
		public void init(ProcessorContext context) {
			this.state = context.getStateStore("myProcessorState");
            context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> System.out.println());
		}

		@Override
		public void process(String key, String value) {
			System.out.println("###key=" + key + ", value=" + value);
		}

		@Override
		public void close() {
			System.out.println("close");
		}
		
	}
}



