package zty.practise.kafka.controller;

import java.time.LocalDateTime;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import zty.practise.kafka.model.RequestEntity;

@RestController
public class ProduceController {

	@Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
	
	private static long count = 0;
	
	@GetMapping("/message/send")
    public String send(@RequestParam String message){
        kafkaTemplate.send("topicA",message);
        return "send success: " + message;
    }
	
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
