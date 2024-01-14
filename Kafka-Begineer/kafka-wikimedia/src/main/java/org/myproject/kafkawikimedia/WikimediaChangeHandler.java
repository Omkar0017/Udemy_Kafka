package org.myproject.kafkawikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
  private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

  KafkaProducer<String, String> kafkaProducer;
  String topic;
  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic){
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  @Override
  public void onOpen()  {
// nothing here
  }

  @Override
  public void onClosed() {
  log.info("-----------------Closing EventHnadler--------------");
  kafkaProducer.close();

  }

  @Override
  public void onMessage(String s, MessageEvent messageEvent) {
    log.info("Message---------->"+messageEvent.getData());
    kafkaProducer.send(new ProducerRecord<>(messageEvent.getData(),topic));
  }

  @Override
  public void onComment(String s) {
// nothing here
  }

  @Override
  public void onError(Throwable throwable) {
  log.error("Error is------>"+throwable.toString());
  }
}
