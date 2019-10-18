package zty.practise.kafka.config;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import zty.practise.kafka.model.RequestEntity;
import zty.practise.kafka.serialize.RequestEntitySerializer;

@Configuration
@EnableKafka
public class KafkaProduceConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String servers;

	@Value("${spring.kafka.producer.retries}")
	private String retries;

	@Value("${spring.kafka.producer.batch-size}")
	private String batchSize;

	@Value("${spring.kafka.producer.acks}")
	private String acks;

	@Bean
	public KafkaTemplate<String, String> kafkaStringStringTemplate() {
		return new KafkaTemplate<>(producerStringStringFactory());
	}

	@Bean
	public ProducerFactory<String, String> producerStringStringFactory() {
		return new DefaultKafkaProducerFactory<>(setStringStringSerializerConfig());
	}
	
	@Bean
	public KafkaTemplate<String, Long> kafkaStringLongTemplate() {
		return new KafkaTemplate<>(producerStringLongFactory());
	}

	@Bean
	public ProducerFactory<String, Long> producerStringLongFactory() {
		return new DefaultKafkaProducerFactory<>(setStringLongSerializerConfig());
	}
	
	@Bean
	public KafkaTemplate<String, Double> kafkaStringDoubleTemplate() {
		return new KafkaTemplate<>(producerStringDoubleFactory());
	}

	@Bean
	public ProducerFactory<String, Double> producerStringDoubleFactory() {
		return new DefaultKafkaProducerFactory<>(setStringDoubleSerializerConfig());
	}
	
	@Bean
	public KafkaTemplate<String, RequestEntity> kafkaStringRequestEntityTemplate() {
		return new KafkaTemplate<>(producerStringRequestEntityFactory());
	}

	@Bean
	public ProducerFactory<String, RequestEntity> producerStringRequestEntityFactory() {
		return new DefaultKafkaProducerFactory<>(setStringRequestEntitySerializerConfig());
	}
	
	@Bean
	public KafkaTemplate<String, String> kafkaStringStringAndInterceptorTemplate() {
		return new KafkaTemplate<>(producerStringStringAndInterceptorFactory());
	}
	
	@Bean
	public ProducerFactory<String, String> producerStringStringAndInterceptorFactory() {
		return new DefaultKafkaProducerFactory<>(setStringStringSerializerAndInterceptorConfig());
	}
	
	/**
	 * 通用配置
	 * 
	 * @param propsMap
	 */
	private void produceComnConfigs(Map<String, Object> propsMap) {
		propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		propsMap.put(ProducerConfig.RETRIES_CONFIG, retries);
		propsMap.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
		propsMap.put(ProducerConfig.ACKS_CONFIG, acks);
	}

	/**
	 * 序列化配置-StringString
	 */
	private Map<String, Object> setStringStringSerializerConfig() {
		Map<String, Object> propsMap = Maps.newHashMap();
		produceComnConfigs(propsMap);

		propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return propsMap;
	}
	
	/**
	 * 序列化配置-StringLong
	 */
	private Map<String, Object> setStringLongSerializerConfig() {
		Map<String, Object> propsMap = Maps.newHashMap();
		produceComnConfigs(propsMap);

		propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
		return propsMap;
	}
	
	/**
	 * 序列化配置-StringDouble
	 */
	private Map<String, Object> setStringDoubleSerializerConfig() {
		Map<String, Object> propsMap = Maps.newHashMap();
		produceComnConfigs(propsMap);

		propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
		return propsMap;
	}
	
	/**
	 * 序列化配置-StringRequestEntity
	 */
	private Map<String, Object> setStringRequestEntitySerializerConfig() {
		Map<String, Object> propsMap = Maps.newHashMap();
		produceComnConfigs(propsMap);

		propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RequestEntitySerializer.class);
		return propsMap;
	}
	
	/**
	 * 序列化配置-StringString并且怎加生产者拦截器
	 */
	private Map<String, Object> setStringStringSerializerAndInterceptorConfig() {
		Map<String, Object> propsMap = Maps.newHashMap();
		produceComnConfigs(propsMap);

		propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		List<String> interceptors = Lists.newArrayList();
		//增加序号的拦截器
		interceptors.add("zty.practise.kafka.interceptor.SeqNumProducerInterceptor"); 
		propsMap.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
		
		return propsMap;
	}
}
