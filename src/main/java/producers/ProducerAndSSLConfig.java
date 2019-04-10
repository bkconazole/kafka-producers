package producers;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAndSSLConfig {

    public static Properties producerProps() {
        //list of bootstrap server(s)
        String bootstarp_servers = "ftwlxkafd001:9092";

        //create producers properties
        Properties properties = new Properties();
        properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarp_servers);
        properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //adding ssl properties
        properties.setProperty("security.protocol", "SSL");
        properties.setProperty("ssl.keystore.password", "T1Bacct1");
        properties.setProperty("ssl.truststore.password", "T1Bacct1");
        properties.setProperty("ssl.keystore.location", "C:/Users/C845601/Desktop/certs/keystore_server_dev.jks");
        properties.setProperty("ssl.truststore.location", "C:/Users/C845601/Desktop/certs/truststore_server_dev.jks");

        //default value of ssl algorithm has changed to https in new kafka upgraded,
        //use empty string to change to the previous mode
        properties.setProperty("ssl.endpoint.identification.algorithm", "");
        return properties;
    }
}
