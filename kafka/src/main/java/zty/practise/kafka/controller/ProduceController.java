package zty.practise.kafka.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class ProduceController {

	@Autowired
    private KafkaTemplate<String, String> kafkaStringStringTemplate;
	
	@Autowired
    private KafkaTemplate<String, Long> kafkaStringLongTemplate;
	
	@Autowired
    private KafkaTemplate<String, Double> kafkaStringDoubleTemplate;
	
	@GetMapping("/message/send")
    public String send(@RequestParam String message){
		kafkaStringStringTemplate.send("topicA",message);
        return "send success: " + message;
    }
	
	@GetMapping("/key/send")
    public String sendKey(@RequestParam String key){
		kafkaStringStringTemplate.send("topicA",key, null);
        return "send success: " +  key;
    }
	
	@GetMapping("/message/sendwithkey")
    public String sendwithkey(@RequestParam String message, @RequestParam String key){
		kafkaStringStringTemplate.send("topicA", key, message);
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
		kafkaStringLongTemplate.send("topicH", key, 3L);
        return "send success: " + key;
    }
	
	@GetMapping("/topicLong/message/send")
    public String sendToLong(String key, String message){
		kafkaStringLongTemplate.send("topicLong", key, StringUtils.isEmpty(message)? null : Long.valueOf(message));
        return "send success: " + message;
    }
	
	@GetMapping("/topicDouble/message/send")
    public String sendToDouble(String key, String message){
		kafkaStringDoubleTemplate.send("topicDouble", key, StringUtils.isEmpty(message)? null : Double.valueOf(message));
        return "send success: " + message;
    }
	
	@GetMapping("/topicTransfer/message/send")
    public String sendToTransfer(String key, String message){
		kafkaStringStringTemplate.send("topicTransfer", key, message);
        return "send success: " + key + message;
    }
}
