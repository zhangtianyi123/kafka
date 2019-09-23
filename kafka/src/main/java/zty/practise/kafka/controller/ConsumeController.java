package zty.practise.kafka.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumeController {

	@KafkaListener(topics = "topicA")
    public void onMessage(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("consume: topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
    }
}
