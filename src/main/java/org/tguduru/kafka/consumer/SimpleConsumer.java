package org.tguduru.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.tguduru.kafka.util.Configuration;

import java.util.Arrays;
import java.util.Properties;

/**
 * A simple {@link org.apache.kafka.clients.consumer.KafkaConsumer} implementation
 */
public class SimpleConsumer {
  public static void main(String[] args) {
    final Properties producerConfig = new Configuration().getProducerProperties();
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(producerConfig);
    kafkaConsumer.subscribe(Arrays.asList("user-orders"));
    while (true) { // its a consumer and needs to run continuously to process incoming data
      ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(100);

      consumerRecord.forEach(a -> System.out.println(a.value()));
    }
  }
}
