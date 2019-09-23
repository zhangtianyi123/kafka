package zty.practise.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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