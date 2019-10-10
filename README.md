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

- consumeA: topic = topicA, offset = 5, value = abcde 
- consumeB: topic = topicB, offset = 2, value = ABCDE 

生产端：
http://localhost:8011/message/send?message=acde

消费端：

- consumeA: topic = topicA, offset = 6, value = acde 


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

- consumeA: topic = topicA, offset = 7, value = aaa 

生产者：
http://localhost:8011/message/send?message=aab

消费者：

- consumeA: topic = topicA, offset = 8, value = aab 
- consumeB: topic = topicB, offset = 3, value = AAB 

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

- 生产端：http://localhost:8011/message/send?message=abc
- 消费端：consumeA: topic = topicA, offset = 13, value = abc 
- 流计算端：@key=null

- 生产端：http://localhost:8011/message/sendwithkey?message=abc&key=001
- 消费端：consumeA: topic = topicA, offset = 14, value = abc 
- 流计算端：@key=001

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

- consumeA: topic = topicA, offset = 15, 
value = {"eventName":"2019-10-07T14:28:14.371","lotName":"15447","procName":"H;+5J","reqId":"0"} 
- consumeD: topic = topicD, offset = 0, value = 15447 


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

- consumeA: topic = topicA, offset = 1, value = this is a hello 
- consumeE: topic = topicE, offset = 57, value = 1 
- consumeE: topic = topicE, offset = 58, value = 1 
- consumeE: topic = topicE, offset = 59, value = 1 
- consumeE: topic = topicE, offset = 60, value = 1

生产者2：（say hello again）
http://localhost:8011/message/send?message=say%20hello%20again

消费者2：

- consumeA: topic = topicA, offset = 2, value = say hello again 
- consumeE: topic = topicE, offset = 61, value = 1 
- consumeE: topic = topicE, offset = 62, value = 2 （key=hello）
- consumeE: topic = topicE, offset = 63, value = 1

### FlatMapApp

使用filterNot-flatMap-flatMapValues等API

需要声明Kafka的源topic的生产者序列化方式为String+Long

```
public class FlatMapApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
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
```

生产者：
http://localhost:8011/longmessage/sendwithkey?key=Hello%20Zhangty

即（“Hello Zhangty”, 3L）

流计算结果：

- key=upper,value=HELLO 
- key=upper,value=ZHANGTY 
- key=lower,value=hello 
- key=lower,value=zhangty 

### MapApp

```
public class MapApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upper_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//"Zhangty", 3L
		KStream<String, Long> simpleFirstStream = builder.stream("topicH", Consumed.with(Serdes.String(), Serdes.Long()));
		
		//map,一对一的转化
		KStream<String, String> mapStream = simpleFirstStream.map((key, value) -> KeyValue.pair(key.toLowerCase(), key.toUpperCase()));
		
		//mapValue，一对一转化，不涉及key
		KStream<String, String> mapValueStream = mapStream.mapValues(value -> "upper" + value);
		
		//peek,与foreach类似，无状态的操作，但不是终端操作不会中断流
		KStream<String, String> peekStream = mapValueStream.peek((key, value) -> System.out.println("peek:key=" + key +", value=" + value));
		
		//selectKey,修改key
		KStream<String, String> keyStream = peekStream.selectKey((key, value) -> value);
		
		keyStream.foreach((key, value) -> System.out.println("print:key=" + key + ",value=" +value));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
```

生产者：
http://localhost:8011/longmessage/sendwithkey?key=Hello

即（“Hello”, 3L）

流计算结果：

- peek:key=hello, value=upperHELLO 
- print:key=upperHELLO,value=upperHELLO

### AggregateApp

聚合可以理解为reduce等的一般化； 通过Aggregate 实现单词计数

前置配置可参考WordCountApp

```
public class AggregateApp {

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

		//无状态的分组操作转为KGroupedStream
		KGroupedStream<String, String> wordGroupedStream = textLines
	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
	            .groupBy((key, word) -> word);
		
		//KGroupedStream执行聚合转为KTable
		KTable<String, Long> wordAggregatedStream = wordGroupedStream.aggregate(
			    () -> 0L, 
			    (aggKey, newValue, aggValue) -> aggValue + 1L,
			    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store")
			    .withValueSerde(Serdes.Long())); 
		
		//KTable -> KStream
		wordAggregatedStream.toStream().to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}

```

测试参考WordCountApp，功能测试结果相同
通过aggregate 实现 count的功能
生产者
http://localhost:8011/message/send?message=this%20is%20a%20hello

发第一次

- consumeA: topic = topicA, offset = 41, value = this is a hello 
- consumeE: topic = topicE, offset = 179, key = this, value = 1
- consumeE: topic = topicE, offset = 180, key = is, value = 1
- consumeE: topic = topicE, offset = 181, key = a, value = 1
- consumeE: topic = topicE, offset = 182, key = hello, value = 1

连发两次consumeA: topic = topicA, offset = 41, value = this is a hello 

- consumeA: topic = topicA, offset = 42, value = this is a hello 
- consumeA: topic = topicA, offset = 43, value = this is a hello 
- consumeE: topic = topicE, offset = 183, key = this, value = 3 
- consumeE: topic = topicE, offset = 184, key = is, value = 3
- consumeE: topic = topicE, offset = 185, key = a, value = 3
- consumeE: topic = topicE, offset = 186, key = hello, value = 3

> 由于默认开启记录缓存，记录的输出结果可能合并，所以不一定能看到value=1-2-3的过程，将看到从1直接变为3

### AggregateApp

通过Aggregate(By Window) 实现单词计数，基于每个窗口对记录值进行聚合

```
public class AggregateWindowApp {

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

		//无状态的分组操作转为KGroupedStream
		KGroupedStream<String, String> wordGroupedStream = textLines
	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
	            .groupBy((key, word) -> word);
		
		//KGroupedStream执行聚合转为KTable
		KTable<Windowed<String>, Long> wordTimeWindowAggregatedStream = wordGroupedStream
				.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
				.aggregate(
			    () -> 0L, 
			    (aggKey, newValue, aggValue) -> aggValue + 1L,
			    Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-store")
			    .withValueSerde(Serdes.Long())); 
		
		//KTable -> KStream
		wordTimeWindowAggregatedStream.toStream()
		.map((k, v) -> new KeyValue<>(k.key(), v))
		.to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
```

生产者：
http://localhost:8011/message/send?message=this%20is%20a%20hello

消费者：

- consumeA: topic = topicA, offset = 42, value = this is a hello         
- consumeA: topic = topicA, offset = 43, value = this is a hello   
- consumeE: topic = topicE, offset = 183, key = this, value = 2, time= 2019-10-08 20:29:34
- consumeE: topic = topicE, offset = 184, key = is, value = 2, time= 2019-10-08 20:29:34  
- consumeE: topic = topicE, offset = 185, key = a, value = 2, time= 2019-10-08 20:29:34  
- consumeE: topic = topicE, offset = 186, key = hello, value = 2, time= 2019-10-08 20:29:34  

<br/>

- consumeA: topic = topicA, offset = 44, value = this is a hello                    
- consumeE: topic = topicE, offset = 187, key = this, value = 3, time= 2019-10-08 20:29:46
- consumeE: topic = topicE, offset = 188, key = is, value = 3, time= 2019-10-08 20:29:46 
- consumeE: topic = topicE, offset = 189, key = a, value = 3, time= 2019-10-08 20:29:46 
- consumeE: topic = topicE, offset = 190, key = hello, value = 3, time= 2019-10-08 20:29:46
<br/>

**基于新的时间窗口重新计数**

- consumeA: topic = topicA, offset = 45, value = this is a hello 
- consumeE: topic = topicE, offset = 191, key = this, value = 1, time= 2019-10-08 20:30:35
- consumeE: topic = topicE, offset = 192, key = is, value = 1, time= 2019-10-08 20:30:35 
- consumeE: topic = topicE, offset = 193, key = a, value = 1, time= 2019-10-08 20:30:35 
- consumeE: topic = topicE, offset = 194, key = hello, value = 1, time= 2019-10-08 20:30:35 

### CountWindowApp

按照每个时间窗口统计分组键key的数量, 通过count(By Window) 实现单词计数

```
public class CountWindowApp {

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

		//无状态的分组操作转为KGroupedStream
		KGroupedStream<String, String> wordGroupedStream = textLines
	            .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
	            .groupBy((key, word) -> word);
		
		//KGroupedStream执行聚合转为KTable
		KTable<Windowed<String>, Long> wordTimeWindowAggregatedStream = wordGroupedStream
				.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
				.count(); 
		
		//KTable -> KStream
		wordTimeWindowAggregatedStream.toStream()
		.map((k, v) -> new KeyValue<>(k.key(), v))
		.to("topicE", Produced.with(Serdes.String(), Serdes.Long()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
	}
}
```

实现与AggregateWindowApp相同的功能

测试：
http://localhost:8011/message/send?message=this%20is%20a%20hello

### ReduceApp

滚动的聚合，通过分组的key组合记录的值，将每一个当前值与上一个reduce的值合并，并返回新的reduce值，**与聚合不同的是，结果值类型无法更改**。

由于reduce无法更改值类型，提前使用map转换key,value值和类型（String -> Long）
由于不修改key,使用groupByKey 替换 groupBy 实现分组

```
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
```

测试：
http://localhost:8011/message/send?message=this%20is%20a%20hello


### ReduceWindowApp

基于时间窗口的reduce操作实现单词计数的功能

相对于非窗口的ReduceApp需要增加windowedBy
进而修改state store的类型Materialized.<String, Long, WindowStore<Bytes, byte[]>>
输出流通过map修改键的类型map((k, v) -> new KeyValue<>(k.key(), v))

```
public class ReduceWindowApp {

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
```

### AggregateTableApp

编写一个球员转会的例子，来展示KTable表的聚合操作

```
public class AggregateTableApp {
	
	private static Map<String, Long> playerAndSalary  = Maps.newHashMap();
	
	static {
		playerAndSalary.put("james", 5000L);
		playerAndSalary.put("kobe", 4000L);
		playerAndSalary.put("davis", 3000L);
	}
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount_app_id");
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
```

按照时间顺序的测试数据以及消费结果：

- james,laker
consumeE: topic = topicE, offset = 227, key = laker, value = 5000
- kobe,cavs
consumeE: topic = topicE, offset = 228, key = cavs, value = 4000
- davis,cavs
consumeE: topic = topicE, offset = 229, key = cavs, value = 7000
- james,cavs
consumeE: topic = topicE, offset = 230, key = laker, value = 0
consumeE: topic = topicE, offset = 231, key = cavs, value = 12000
- davis,null
consumeE: topic = topicE, offset = 232, key = cavs, value = 9000
- null,laker
[ignore]
- kobe,laker
consumeE: topic = topicE, offset = 233, key = cavs, value = 5000
consumeE: topic = topicE, offset = 234, key = laker, value = 4000

过程说明图：

![image_1dmo5gg2v1kntevia08svd1rq69.png-23.5kB][1]

### StreamStreamInnerJoinApp

KStream-KStream的链接始终是窗口连接，因执行连接内部的状态存储将无限增长

一侧的一条记录及那个会匹配另一侧每一个匹配的记录形成联结输出

在时间窗口内，左右键key相等，才会出发连接进而根据ValueJoiner形成输出
具有空键或空值的输入记录将被忽略，并且不会触发联接

innerjoin只会连接key相同匹配到的记录


```
public class StreamStreamInnerJoinApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamstreaminnerjoin_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//源topic
		KStream<String, Long> leftStream = builder.stream("topicLong", Consumed.with(Serdes.String(), Serdes.Long()));
		KStream<String, Double> rightStream = builder.stream("topicDouble", Consumed.with(Serdes.String(), Serdes.Double()));
		
		KStream<String, String> joinedStream = leftStream.join(rightStream,
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
```

由于涉及多个不同类型的输入源，所以自定义多个KafkaTemplate

```
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
	
	//StringLong
	//StringDouble
	//...
}
```

在时间窗口内的连接测试：

生产者（按照时间顺序发送）：

- http://localhost:8011/topicLong/message/send?key=a&message=1 （left流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.1  （right流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.2  （right流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.3  （right流）
- http://localhost:8011/topicLong/message/send?key=a&message=2  （left流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.4  （right流）
- http://localhost:8011/topicLong/message/send?key=b&message=2  （left流,匹配不到right流）
- http://localhost:8011/topicDouble/message/send?key=c&message=1.4  （right流，匹配不到left流）

消费者：

- consumeA: topic = topicA, value = left=1, right=1.1 
- consumeA: topic = topicA, value = left=1, right=1.2 
- consumeA: topic = topicA, value = left=1, right=1.3 
- consumeA: topic = topicA, value = left=2, right=1.1 
- consumeA: topic = topicA, value = left=2, right=1.2 
- consumeA: topic = topicA, value = left=2, right=1.3 
- consumeA: topic = topicA, value = left=1, right=1.4 
- consumeA: topic = topicA, value = left=2, right=1.4 

生产者发送的最后两条左流匹配不到右流和右流匹配不到左流的记录均不连接

### StreamStreamLeftJoinApp

由于是流与流的连接，仍然需要是基于窗口的

left join与inner join 最大的区别就是左边流的记录key匹配不到右边的key的时候，将会触发
ValueJoiner#apply(leftRecord.value, null)
即只要左流有，不管右流有没有都匹配，没有则匹配空

使用leftJoin方法
```
KStream<String, String> joinedStream = leftStream.leftJoin(rightStream,
			    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
			    JoinWindows.of(TimeUnit.MINUTES.toMillis(2)),
			    Joined.with(
			      Serdes.String(), /* key */
			      Serdes.Long(),   /* left value */
			      Serdes.Double())  /* right value */
			  );
```
生产者（按照时间顺序发送）：

- http://localhost:8011/topicLong/message/send?key=a&message=1 （left流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.1  （right流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.2  （right流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.3  （right流）
- http://localhost:8011/topicLong/message/send?key=a&message=2  （left流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.4  （right流）
- **http://localhost:8011/topicLong/message/send?key=b&message=2**  （left流,匹配不到right流）
- http://localhost:8011/topicDouble/message/send?key=c&message=1.4  （right流，匹配不到left流）

消费者：

- consumeA: topic = topicA, value = left=1, right=1.1 
- consumeA: topic = topicA, value = left=1, right=1.2 
- consumeA: topic = topicA, value = left=1, right=1.3 
- consumeA: topic = topicA, value = left=2, right=1.1 
- consumeA: topic = topicA, value = left=2, right=1.2 
- consumeA: topic = topicA, value = left=2, right=1.3 
- consumeA: topic = topicA, value = left=1, right=1.4 
- consumeA: topic = topicA, value = left=2, right=1.4 
- **consumeA: topic = topicA, value = left=2, right=null** 

### StreamStreamOuterJoinApp

由于是流与流的连接，仍然需要是基于窗口的

outer join与inner join/left join 最大的区别就是

1.左边流的记录key匹配不到右边的key的时候，将会触发ValueJoiner#apply(leftRecord.value, null)
即只要左流有，不管右流有没有都匹配，没有则匹配空
2. 右边流的记录key匹配步到左边的key的时候，将会触发ValueJoiner#apply(null, rightRecord.value)
即只要右流有，不管左流有没有都匹配，没有则匹配空


使用outerJoin方法
```
KStream<String, String> joinedStream = leftStream.outerJoin(rightStream,
			    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
			    JoinWindows.of(TimeUnit.MINUTES.toMillis(2)),
			    Joined.with(
			      Serdes.String(), /* key */
			      Serdes.Long(),   /* left value */
			      Serdes.Double())  /* right value */
			  );
```

生产者（按照时间顺序发送）：

- http://localhost:8011/topicLong/message/send?key=a&message=1 （left流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.1  （right流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.2  （right流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.3  （right流）
- http://localhost:8011/topicLong/message/send?key=a&message=2  （left流）
- http://localhost:8011/topicDouble/message/send?key=a&message=1.4  （right流）
- **http://localhost:8011/topicLong/message/send?key=b&message=2**  （left流,匹配不到right流）
- **http://localhost:8011/topicDouble/message/send?key=c&message=1.4**  （right流，匹配不到left流）

消费者：

- consumeA: topic = topicA, value = left=1, right=1.1 
- consumeA: topic = topicA, value = left=1, right=1.2 
- consumeA: topic = topicA, value = left=1, right=1.3 
- consumeA: topic = topicA, value = left=2, right=1.1 
- consumeA: topic = topicA, value = left=2, right=1.2 
- consumeA: topic = topicA, value = left=2, right=1.3 
- consumeA: topic = topicA, value = left=1, right=1.4 
- consumeA: topic = topicA, value = left=2, right=1.4 
- **consumeA: topic = topicA, value = left=2, right=null** 
- **consumeA: topic = topicA, value = left=null, right=1.4** 

<br />

**KTable-KTable Inner Join**

表表连接总是基于非窗的，连接的结果是一个新的KTable
这个结果表是一个不断更新的结果表，存储最新结果

两个KTable 的changelog 流具体化到本地存储中，表示最新快照

当输入的记录key为null的时候，忽略记录，不触发连接
当输入的记录value为null的时候，表示表删除了键，不会触发连接，如果结果表已经存在这个key，将转发到结果表

```
public class TableTableInnerJoinApp {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tabletableinnerjoin_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.192.202:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		//源topic
		KTable<String, Long> leftTable = builder.table("topicLong", Consumed.with(Serdes.String(), Serdes.Long()));
		KTable<String, Double> rightTable = builder.table("topicDouble", Consumed.with(Serdes.String(), Serdes.Double()));
		
		KTable<String, String> joinedTable = leftTable.join(rightTable,
			    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
			    Materialized.with(Serdes.String(), Serdes.String()));
		
		joinedTable.toStream().to("topicA", Produced.with(Serdes.String(), Serdes.String()));
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
	}
}
```

- 左流生产(a,1)
- 右流生产(a,1.1)
- 消费：consumeA: topic = topicA, value = left=1, right=1.1 
- 右流生产(a,1.2)
- 消费：consumeA: topic = topicA, value = left=1, right=1.2 
- 右流生产(a,1.3)
- 左流生产(a,2)
- 消费：consumeA: topic = topicA, value = left=2, right=1.3 
- 右流生产(a,null)
- 消费：consumeA: topic = topicA, value = null
- 左流生产(a,2)
- 消费：无


- 结果说明
 - 第一次左流生产(a,2)的时候匹配的不是1.1 1.2 1.3 三条而是最新的1.3 这是表的更新特性
 - 第一次左流生产(a,2)的时候直接产生（2，1.3）没有产生（1，1.3）因为缓存的存在，可能不会输出中间（过期）的连接状态
 - 右流生产(a,null)的时候将数据a删除了，所以后面左流最后一次生产(a,2)匹配不到





  [1]: http://static.zybuluo.com/zhangtianyi/e6quts74wyf2ljunkmmng90b/image_1dmo5gg2v1kntevia08svd1rq69.png