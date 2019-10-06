package zty.practise.kafka.stream.serde;


import org.apache.kafka.common.serialization.Serde;

import zty.practise.kafka.model.RequestEntity;
import zty.practise.kafka.serialize.JsonDeserializer;
import zty.practise.kafka.serialize.JsonSerializer;

public class SerdeUtil {
	
	public static Serde<RequestEntity> RequestEntitySerde() {
		return new RequestEntitySerde();
	}

	public static final class RequestEntitySerde extends WrapperSerde<RequestEntity> {
		public RequestEntitySerde() {
			super(new JsonSerializer<RequestEntity>(), new JsonDeserializer<RequestEntity>(RequestEntity.class));
		}
	}

}
