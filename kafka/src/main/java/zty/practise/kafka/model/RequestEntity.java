package zty.practise.kafka.model;

import java.io.Serializable;

import lombok.Data;

@Data
public class RequestEntity  implements Serializable {

	private static final long serialVersionUID = -2456101186707344549L;

	private String reqId;
	
	private String lotName;
	
	private String procName;
	
	private String opName;
	
	private String eventName;
}
