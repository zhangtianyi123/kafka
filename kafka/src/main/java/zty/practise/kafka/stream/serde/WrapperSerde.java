package zty.practise.kafka.stream.serde;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
 
public class WrapperSerde<T> implements Serde<T> {

	final private Serializer<T> serializer;
    final private Deserializer<T> deserializer;
 
    WrapperSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
    	this.serializer = serializer;
    	this.deserializer = deserializer;
    }
 
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    	serializer.configure(configs, isKey);
    	deserializer.configure(configs, isKey);
    }
 
    @Override
    public Serializer<T> serializer() {
    	return serializer;
    }
 
    @Override
	public Deserializer<T> deserializer() {
		return deserializer;	
	}

	@Override	
	public void close() {
		serializer.close();
		deserializer.close();
	}
 
}
