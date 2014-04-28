package org.kafka.producer;

import org.kafka.util.ConfigImpl;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * A simple {@link Producer} implementation
 */
public class SimpleProducer {
    private static final ProducerConfig producerConfig = ConfigImpl.getProducerConfig();

    public static void main(final String args[]) {
        final Producer<Integer, String> producer = new Producer<Integer, String>(producerConfig);
        final KeyedMessage<Integer, String> message = new KeyedMessage<Integer, String>("demo", "First Message");
        producer.send(message);
        producer.close();
        System.out.println("Message Sent");
    }
}
