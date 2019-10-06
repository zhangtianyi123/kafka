package zty.practise.kafka.serialize;

import zty.practise.kafka.model.RequestEntity;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import com.alibaba.fastjson.JSON;


public class RequestEntitySerializer implements Serializer<RequestEntity> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, RequestEntity requestEntity) {
        return JSON.toJSONBytes(requestEntity);
    }

    @Override
    public void close() {
    }
}
