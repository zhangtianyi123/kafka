package zty.practise.kafka.serialize;

import zty.practise.kafka.model.RequestEntity;
import org.apache.kafka.common.serialization.Deserializer;
import com.alibaba.fastjson.JSON;
import java.util.Map;


public class RequestEntityDeserializer implements Deserializer<RequestEntity> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public RequestEntity deserialize(String s, byte[] bytes) {
        return JSON.parseObject(bytes, RequestEntity.class);
    }

    @Override
    public void close() {

    }
}
