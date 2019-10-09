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
	
	@GetMapping("/key/send")
    public String sendKey(@RequestParam String key){
        kafkaTemplate.send("topicA",key, null);
        return "send success: " +  key;
    }
	
	@GetMapping("/message/sendwithkey")
    public String sendwithkey(@RequestParam String message, @RequestParam String key){
        kafkaTemplate.send("topicA", key, message);
        return "send success: " + message;
    }
	
	/**
	 * (String key, Long message)
	 * @param message
	 * @param key
	 * @return
	 */
	@GetMapping("/longmessage/sendwithkey")
    public String sendLongMessagewithkey(@RequestParam String key){
        kafkaTemplate.send("topicH", key, 3L);
        return "send success: " + key;
    }
}
