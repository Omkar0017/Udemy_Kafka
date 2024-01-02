package org.conductor.io;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerDemoWithShutdown {

  private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

  public static void main(String[] args) {

    log.info("Started Consumption ....");
    String topic = "first_topic";

    var props = consumerProps();

    //Creating a Consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

    try{


      //get a reference to the main thread
      final Thread mainThread = Thread.currentThread();

      // Adding the shutdown hook

      Runtime.getRuntime().addShutdownHook(new Thread(){
      public void run(){
        log.info("Detected a shutdown, let's exit by calling consumer.wakeup().....");
        consumer.wakeup();

        //join the main thread to allow the execution of code in main thread

        try {
          mainThread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }
      });

      //Subscribe to the topic
      consumer.subscribe(Arrays.asList(topic,"second_topic"));

      //poll the data
      while(true){
        log.info("Polling ....");

        ConsumerRecords<String,String> records =
            consumer.poll(Duration.ofSeconds(1));

        for ( ConsumerRecord<String, String> record : records){

          log.info("Key---->"+record.key() + " Value--------->"+record.value());
          log.info("Partition-------->"+ record.partition() + " OffSet------->"+record.offset());

        }

      }

    } catch (WakeupException we){
      log.info("Shutting down consumer");
    }
    catch (Exception e){
      log.error("Error----->"+e.toString());
    }
    finally {
      consumer.close();
      log.info("Consumer id gracefully shutting down");
    }
  }


  public static Properties consumerProps(){
    var props = new Properties();
    String group_id = "my_group_id";
    props.put("bootstrap.servers", "renewed-sponge-6078-eu1-kafka.upstash.io:9092");
    props.put("sasl.mechanism", "SCRAM-SHA-256");
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cmVuZXdlZC1zcG9uZ2UtNjA3OCRTY9z55KGZD79IK90FL8C2PcW7au14EZ-awuw\" password=\"NDZlZjBkZWMtYTE0YS00ZGIxLWExYTktNmZiMGY1OGM2YWJj\";");

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");
    props.put("group.id", group_id);

    return props;
  }
}
