package Tests;

import org.apache.kafka.clients.producer.*;

import java.util.HashMap;
import java.util.Map;

public class KafakIntSender {
    public static void main(String[] args) {
//        System.out.println(saySomething());
        // TODO 增加 HTTP 服务端，实现分辨率、帧率的客户端读取
        // 能设置更好

        Map<String,Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            while (true){
                System.out.println("send");
                String num = String.valueOf((int)(1+Math.random()*(100-1+1)));
                producer.send(new ProducerRecord<String, String>("test", num + "\n"));
                Thread.sleep(10);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
