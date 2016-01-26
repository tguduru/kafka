package org.tguduru.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.tguduru.avro.schema.Product;
import org.tguduru.avro.schema.ProductType;
import org.tguduru.avro.serilization.AvroWriter;
import org.tguduru.kafka.util.Configuration;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * A simple {@link Producer} implementation
 */
public class SimpleProducer {

    private ProducerConfig producerConfig;

    public static void main(final String args[]) throws IOException {
        final Properties producerConfig = new Configuration().getProducerProperties();
        final KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<String, byte[]>(
                producerConfig);
        final Product product = Product.newBuilder().setName("Bose").setDescription("Music Head Phones")
                .setPrice(100.0).setProductType(ProductType.Electronics).build();
        final AvroWriter avroWriter = new AvroWriter();
        final ByteArrayOutputStream byteArrayOutputStream = avroWriter.serialize(product);
        final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
        byteArraySerializer.serialize("spark-kafka",byteArrayOutputStream.toByteArray());
        final ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(
                "spark-kafka", "product", byteArrayOutputStream.toByteArray());
        kafkaProducer.send(record);
        kafkaProducer.close();
        System.out.println("Message Sent");
    }
}
