package Util;
import kafka.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;


import java.util.Properties;

public class TestForeachWriter extends ForeachWriter<Row> {

    private Properties kafkaProperties;
    private KafkaProducer kafkaProducer;
    private String topic;


    public TestForeachWriter(String server,String topic) {
        this.kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", server);
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.topic = topic;
    }



    @Override
    public boolean open(long partitionId, long version) {
        kafkaProducer = new KafkaProducer(kafkaProperties);
        return true;
    }

    @Override
    public void process(Row row) {
        kafkaProducer.send(new ProducerRecord(this.topic, row.toString()));
    }

    @Override
    public void close(Throwable arg0) {
        kafkaProducer.close();
    }
}
