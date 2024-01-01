package org.conductor.io;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ProducerDemo {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

  public static void main(String[] args) {

    log.info("Hwllo World");

    try {
      //Creating Producer Properties
      Properties props = producerProps();

      //Creating a producer

      KafkaProducer<String, String> producer = new KafkaProducer<>(props);

      //Create a Producer Record
      ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World");

      // Send Data
      producer.send(record);

      //Flush amd close producer

      producer.flush();
      producer.close();

    }catch (Exception e){
      log.error("Error----->"+e.toString());
    }
  }


  public static Properties producerProps(){
    var props = new Properties();
    props.put("bootstrap.servers", "https://renewed-sponge-6078-eu1-kafka.upstash.io:9092");
    props.put("sasl.mechanism", "SCRAM-SHA-256");
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cmVuZXdlZC1zcG9uZ2UtNjA3OCRTY9z55KGZD79IK90FL8C2PcW7au14EZ-awuw\" password=\"NDZlZjBkZWMtYTE0YS00ZGIxLWExYTktNmZiMGY1OGM2YWJj\";");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return props;
  }
}
