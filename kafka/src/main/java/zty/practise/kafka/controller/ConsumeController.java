package zty.practise.kafka.controller;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumeController {

	@KafkaListener(topics = "topicA", containerFactory="myListenerContainerFactory")
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
	
	@KafkaListener(topics = "topicD", containerFactory="myListenerContainerFactory")
    public void onMessageD(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("consumeD: topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
    }
	
	@KafkaListener(topics = "topicE")
    public void onMessageE(ConsumerRecord<?, ?> record) throws Exception {
		String strDateFormat = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
        System.out.printf("consumeE: topic = %s, offset = %d, key = %s, value = %s, time= %s \n", 
        		record.topic(), record.offset(), record.key(), record.value(), sdf.format(new Date(record.timestamp())));
    }
}
