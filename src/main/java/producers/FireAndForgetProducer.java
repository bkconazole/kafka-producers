package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 *this approach sends a message to the server and don't really care if it arrives successfully or not
 *most of the time, messages will be delivered since the kafka producer api will retry to send the message
 *some messages will get lost using this approach
*/
public class FireAndForgetProducer {

    public static void main(String[] args) {

        String topic = "com.bnsf.kiv.brook.test.concurrency3";
        String message = "hello kafka, i love you";

        Properties properties = ProducerAndSSLConfig.producerProps();

        // create the producers
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create the producers record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);

        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //flush data
        producer.flush();

        //flush and close producers
        producer.close();
    }
}
