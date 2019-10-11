package zty.practise.kafka.model;

import java.io.Serializable;

import lombok.Data;

//@Data
public class RequestEntity  implements Serializable {

	private static final long serialVersionUID = -2456101186707344549L;

	private String reqId;
	
	private String lotName;
	
	private String procName;
	
	private String opName;
	
	private String eventName;

	public String getReqId() {
		return reqId;
	}

	public void setReqId(String reqId) {
		this.reqId = reqId;
	}

	public String getLotName() {
		return lotName;
	}

	public void setLotName(String lotName) {
		this.lotName = lotName;
	}

	public String getProcName() {
		return procName;
	}

	public void setProcName(String procName) {
		this.procName = procName;
	}

	public String getOpName() {
		return opName;
	}

	public void setOpName(String opName) {
		this.opName = opName;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
}
