# Kafka and Kafka Stream

---

安装，启动kafka （创建分区，开启防火墙等工作略）

```
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties
jps
```

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topicA
bin/kafka-topics.sh --list --zookeeper localhost:2181
```


测试字符串的发送与接收,指定序列化器和反序列化器
```
spring:
  kafka:
    bootstrap-servers: 192.168.192.202:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

springboot通过属性文件（applicaiton.yml）自动装配kafkaTemplate等，简易的生产消息如下（发送到）topicA主题

```
@RestController
public class ProduceController {

	@Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
	
	@GetMapping("/message/send")
    public String send(@RequestParam String message){
        kafkaTemplate.send("topicA",message);
        return "send success: " + message;
    }
}
```

消费者监听topicA主题

```
@RestController
public class ConsumeController {

	@KafkaListener(topics = "topicA")
    public void onMessageA(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("consumeA: topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
    }
```

发送与结果：
生产端：http://localhost:8011/message/send?message=asdcd
消费端：consumeA: topic = topicA, offset = 0, value = asdcd

### StreamUpperApp

启动Kafka Stream StreamUpperApp应用，通过**mapValues**执行转为大写的转换
再通过**filter**过滤掉不包含B字符的记录

```
public class StreamUpperApp {

	/**
	 * 处理1：小写转大写
	 * 处理2：过滤掉不包含B字符的记录
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> simpleFirstStream = builder.stream("topicA");

		ForeachAction<String, String> purchaseForeachAction = (key, value) -> System.out.println(key);
		
		
		KStream<String, String> upperStream = simpleFirstStream.mapValues(s -> s.toUpperCase());
		upperStream.filter((key, value) -> value.contains("B"))
		//.foreach(purchaseForeachAction)
		.to("topicB");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
```

生产端：
http://localhost:8011/message/send?message=abcde
消费端：
consumeA: topic = topicA, offset = 5, value = abcde 
consumeB: topic = topicB, offset = 2, value = ABCDE 

生产端：
http://localhost:8011/message/send?message=acde
消费端：
consumeA: topic = topicA, offset = 6, value = acde 


### StreamUpperAndBranchApp

启动Kafka Stream StreamUpperAndBranchApp，通过**mapValues**执行转为大写的转换
再通过**branch**根据含有B/C字符条件分支到不同的流

```
public class StreamUpperAndBranchApp {

	/**
	 * 处理1：小写转大写
	 * 处理2：根据含有B/C字符条件分支到不同的流
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> simpleFirstStream = builder.stream("topicA");

		KStream<String, String> upperStream = simpleFirstStream.mapValues(s -> s.toUpperCase());
		Predicate<String, String> predicateB = (key, value) -> value.contains("B");
		Predicate<String, String> predicateC = (key, value) -> value.contains("C");
		
		KStream<String, String>[] branchs = upperStream.branch(predicateB, predicateC);
		branchs[0].to("topicB");
		branchs[1].to("topicC");
		

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
```

生产者：
http://localhost:8011/message/send?message=aaa

消费者：
consumeA: topic = topicA, offset = 7, value = aaa 

生产者：
http://localhost:8011/message/send?message=aab

消费者：
consumeA: topic = topicA, offset = 8, value = aab 

consumeB: topic = topicB, offset = 3, value = AAB 

生产者：
http://localhost:8011/message/send?message=aac

消费者：
consumeA: topic = topicA, offset = 9, value = aac 

consumeC: topic = topicC, offset = 0, value = AAC 

### StreamUpperForeachApp

foreach是终端操作和to不兼容，比如打印，比如输出到内存缓存nosql/保存到db等等

通过foreach操作打印每个消息的key 
```
public class StreamUpperForeachApp {
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> simpleFirstStream = builder.stream("topicA");

		ForeachAction<String, String> purchaseForeachAction = (key, value) -> System.out.println("@key=" + key);
		
		
		KStream<String, String> upperStream = simpleFirstStream.mapValues(s -> s.toUpperCase());
		upperStream.filter((key, value) -> value.contains("B"))
		.foreach(purchaseForeachAction);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
```

生产端：http://localhost:8011/message/send?message=abc

消费端：consumeA: topic = topicA, offset = 13, value = abc 

流计算端：@key=null

生产端：http://localhost:8011/message/sendwithkey?message=abc&key=001

消费端：consumeA: topic = topicA, offset = 14, value = abc 

流计算端：@key=001

### StreamUpperEntityApp

调整生产者的序列化器配置，制定实体的序列化器（通过Json的方式序列化）
json方式虽然紧凑型不足，但是使用简便性好

```
spring:
  kafka:
    bootstrap-servers: 192.168.192.202:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: zty.practise.kafka.serialize.RequestEntitySerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

生产实体消息

```@RestController
public class ProduceEntityController {

	@Autowired
    private KafkaTemplate<String, RequestEntity> kafkaTemplate;
	
	private static long count = 0;
	
	@GetMapping("/send")
    public String requestSend(){
		RequestEntity entity = new RequestEntity();
		entity.setEventName(LocalDateTime.now().toString());
		entity.setLotName(RandomStringUtils.randomNumeric(5));
		entity.setProcName(RandomStringUtils.randomAscii(5));
		entity.setReqId((count++) + "");
		
        kafkaTemplate.send("topicA", entity);
        return "send success: " + entity;
    }
}
```

Kafka Stream 提取属性中的字符串

```
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
```

生产者：http://localhost:8011/send

消费者：
consumeA: topic = topicA, offset = 15, value = {"eventName":"2019-10-07T14:28:14.371","lotName":"15447","procName":"H;+5J","reqId":"0"} 

consumeD: topic = topicD, offset = 0, value = 15447 


### WordCountApp

修改yml配置为生产者字符串序列化器

```
spring:
  kafka:
    bootstrap-servers: 192.168.192.202:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringSerializer
```

手动声明另一种序列化配置的消费监听工厂
```
@Configuration
@EnableKafka
public class KafkaConsumeConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String servers;

	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;

	@Value("${spring.kafka.consumer.auto-offset-reset}")
	private String autoOffsetReset;

	@Value("${spring.kafka.consumer.enable-auto-commit}")
	private String enableAutoCommit;

	@Value("${spring.kafka.consumer.auto-commit-interval}")
	private String autoCommitInterval;

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Long>> myListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Long> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.getContainerProperties().setPollTimeout(1500);
		return factory;
	}

	public ConsumerFactory<String, Long> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	public Map<String, Object> consumerConfigs() {
		Map<String, Object> propsMap = Maps.newHashMap();
		propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
		propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
		propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		return propsMap;
	}
}
```

制定监听TopicA的特殊配置
```
@RestController
public class ConsumeController {

	@KafkaListener(topics = "topicA", containerFactory="myListenerContainerFactory")
    public void onMessageA(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("consumeA: topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
    }
}
```

分析程序
count是有状态的聚合操作，将会生成KTable
表流具有二像性，将insertOrUpdate的表再转化为追加insert的changelog流输出
```
public class WordCountApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		//消息key-value对的默认序列化和反序列
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//源topic
		KStream<String, String> textLines = builder.stream("topicA");

		KTable<String, Long> wordCounts = textLines
	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
	            .groupBy((key, word) -> word)
	            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
		wordCounts.toStream().to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
```


生产者1：（this ia a hello）
http://localhost:8011/message/send?message=this%20is%20a%20hello

消费者1：
consumeA: topic = topicA, offset = 1, value = this is a hello 

consumeE: topic = topicE, offset = 57, value = 1 

consumeE: topic = topicE, offset = 58, value = 1 

consumeE: topic = topicE, offset = 59, value = 1 

consumeE: topic = topicE, offset = 60, value = 1

生产者2：（say hello again）
http://localhost:8011/message/send?message=say%20hello%20again

消费者2：
consumeA: topic = topicA, offset = 2, value = say hello again 

consumeE: topic = topicE, offset = 61, value = 1 

consumeE: topic = topicE, offset = 62, value = 2 （key=hello）

consumeE: topic = topicE, offset = 63, value = 1