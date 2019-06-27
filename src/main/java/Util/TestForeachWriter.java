package Util;
import kafka.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;


import java.util.Properties;

public class TestForeachWriter extends ForeachWriter<Row> {

    public Properties kafkaProperties;
    public KafkaProducer kafkaProducer;

    public TestForeachWriter() {
        this.kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    }



    @Override
    public boolean open(long partitionId, long version) {
        kafkaProducer = new KafkaProducer(kafkaProperties);
        return true;
    }

    @Override
    public void process(Row row) {
        kafkaProducer.send(new ProducerRecord("test1", row.get(0)+":"+row.get(1)));
    }

    @Override
    public void close(Throwable arg0) {
        kafkaProducer.close();
    }
}
