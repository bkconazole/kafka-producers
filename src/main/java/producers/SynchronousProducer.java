package producers;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * send message of kafka's producer api returns a Future object
 * this approach uses get() to wait on the future and see if the send() was successful or not
 */
public class SynchronousProducer {

    public static void main(String[] args) {

        //topic where you want to send the message to
        String topic = "com.bnsf.kiv.brook.test.concurrency3";

        //the message you want to publish
        String value = "hi, there kafka, i still love you 2";
        //get configs from ProducerAndSSLConfig class
        Properties properties = ProducerAndSSLConfig.producerProps();

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

        //send a message
        try {
            //get() waits for a reply from kafka, and will throw exception if the record is not sent successfully
            //if no errors, it returns a RecordMetadata
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
