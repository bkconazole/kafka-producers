package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerSample {

    public static void main(String[] args) {

        String topic = "com.bnsf.kiv.brook.test.concurrency3";                  //kafka topic name
        String value = "hello kafka, i love you";   //message that you want to send
        //list of bootstrap servers
        String bootstarp_servers = "ftwlxkafd001:9092,ftwlxkafd002:9092,ftwlxkafd003:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarp_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //adding ssl properties
        properties.setProperty("security.protocol", "SSL");
        properties.setProperty("ssl.keystore.password", "*");
        properties.setProperty("ssl.truststore.password", "*");
        properties.setProperty("ssl.keystore.location", "C:/Users/C845601/Desktop/certs/keystore_server_dev.jks");
        properties.setProperty("ssl.truststore.location", "C:/Users/C845601/Desktop/certs/truststore_server_dev.jks");
        //default value of
        properties.setProperty("ssl.endpoint.identification.algorithm", "");

        //properties.setProperty("ssl.enabled.password", "T1Bacct1");
        //properties.setProperty()

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create the producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

        //send data asynchronous
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
