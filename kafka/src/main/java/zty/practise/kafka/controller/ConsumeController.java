package zty.practise.kafka.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumeController {

	@KafkaListener(topics = "topicA")
    public void onMessageA(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("consumeA: topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
    }
	
	
	@KafkaListener(topics = "topicB")
    public void onMessageB(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("consumeB: topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
    }
	
	@KafkaListener(topics = "topicC")
    public void onMessageC(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("consumeC: topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
    }
}
