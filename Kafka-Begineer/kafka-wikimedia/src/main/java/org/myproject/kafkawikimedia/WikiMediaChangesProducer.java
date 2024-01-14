package org.myproject.kafkawikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventSource.Builder;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class WikiMediaChangesProducer {
  private static final Logger log = LoggerFactory.getLogger(WikiMediaChangesProducer.class.getSimpleName());

  public static void main(String[] args) throws InterruptedException {
    log.info(WikiMediaChangesProducer.class.getSimpleName() + " --------Started-----");

    KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
    String topic = "wiki_update";
    String url = "https://stream.wikimedia.org/v2/stream/recentchange";

    EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
    EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
    EventSource eventSource = builder.build();

    //Start producer in another thread

    eventSource.start();

    //Add time to block the program until then
    TimeUnit.SECONDS.sleep(30);



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



