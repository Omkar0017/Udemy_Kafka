package org.conductor.io;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerDemoWithDiffPartition {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithDiffPartition.class.getSimpleName());

  public static void main(String[] args) {

    log.info("Producer Starting....");

    try {
      //Creating Producer Properties
      Properties props = producerProps();

      //Creating a producer

      KafkaProducer<String, String> producer = new KafkaProducer<>(props);


    for (int j=0;j<=10; j++) {
      for (int i = 0; i <= 30; i++) {
        //Create a Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("second_topic",
            "Hello World" + i);
        // Send Data
        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
              log.info("Received new MetaData \n"
                  + "Topic: " + recordMetadata.topic() + "\n"
                  + "Partition: " + recordMetadata.partition() + "\n"
                  + "OffSet: " + recordMetadata.offset() + "\n"
                  + "TimeStamp: " + recordMetadata.timestamp() + "\n"
              );
            } else {
              log.error("Error While Producing------->", e);
            }
          }
        });


      }

      Thread.sleep(500);
    }
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
//    props.put("batch.size","400");
//    props.put("partitioner.class", RoundRobinPartitioner.class.getName());

    return props;
  }
}
