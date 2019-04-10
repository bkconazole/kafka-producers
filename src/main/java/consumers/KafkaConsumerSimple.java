package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerSimple {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaConsumerSimple.class.getName());

        String auto_offset_reset = "latest";      //earliest, none, latest
        String topic = "com.bnsf.kiv.brook.test.concurrency3";
        String group_id = "sample-consumer";
        String bootstrap_servers = "ftwlxkafd001:9092";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, auto_offset_reset);
        properties.setProperty (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty (ConsumerConfig.GROUP_ID_CONFIG, group_id);

        //create ssl properties
        properties.setProperty("security.protocol", "SSL");
        properties.setProperty("ssl.keystore.password", "T1Bacct1");
        properties.setProperty("ssl.truststore.password", "T1Bacct1");
        properties.setProperty("ssl.keystore.location", "C:/Users/C845601/Desktop/certs/keystore_server_dev.jks");
        properties.setProperty("ssl.truststore.location", "C:/Users/C845601/Desktop/certs/truststore_server_dev.jks");

        //default value of ssl algorithm has changed to https in new kafka upgraded,
        // use empty string to change to the previous mode
        properties.setProperty("ssl.endpoint.identification.algorithm", "");

        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe the consumer to topic(s)
        consumer.subscribe(Collections.singleton(topic));

        //poll messages (consumer has to ask for data)
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            //iterate over each record and log desired data
            for (ConsumerRecord<String, String> record : records)  {
                logger.info("key: " + record.key() + " value: " + record.value());
                logger.info("partition: " + record.value() + " offset: " + record.offset());
            }
        }
    }
}
