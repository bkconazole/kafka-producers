package producers;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * call the send() with a callback function, which gets triggered when it receives a response from
 * kafka broker
 */
public class AsynchronousProducer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(AsynchronousProducer.class.getName());

        String topic = "com.bnsf.kiv.brook.test.concurrency3";
        String message = "hello kafka, i love you";
        int number_of_messages = 10;

        Properties properties = ProducerAndSSLConfig.producerProps();

        // create the producers
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        //send multiple records
        for (int i=1; i <= number_of_messages; ++i) {

            //create the producers record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message + Integer.toString(i));

            //send data asynchronous using a callback
            producer.send(record, new Callback() {
                @Override
                //executes every time a record is being sent successfully or an exception is thrown
                public void onCompletion(RecordMetadata metadata, Exception e) {

                    //when a record is successfully sent, onCompletion() will have a nonnull exception
                    if (e == null) {
                        logger.info(String.format( "%n new record metadata is recieved%n topic: %s%n partition: %d%n offset: %d%n" +
                                " timestamp: %d%n", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()
                        ));
                    }

                    //an exception is throwing while a record is sent
                    else {
                        logger.error("something went awry while sending a record", e);
                    }
                }
            });
        }

        //flush data
        producer.flush();

        //flush and close producers
        producer.close();
    }
}
