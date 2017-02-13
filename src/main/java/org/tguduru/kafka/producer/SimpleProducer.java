package org.tguduru.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.tguduru.avro.schema.Product;
import org.tguduru.avro.schema.ProductType;
import org.tguduru.avro.serilization.AvroWriter;
import org.tguduru.kafka.util.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A simple {@link KafkaProducer} implementation
 */
public class SimpleProducer {


  public static void main(final String args[]) throws IOException, ExecutionException,
      InterruptedException {
    final Properties producerConfig = new Configuration().getProducerProperties();
    final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig);
    final Product product =
        Product.newBuilder().setName("Bose").setDescription("Music Head Phones").setPrice(100.0)
            .setProductType(ProductType.Electronics).build();
    final AvroWriter avroWriter = new AvroWriter();
    final ByteArrayOutputStream byteArrayOutputStream = avroWriter.serialize(product);
    final ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    byteArraySerializer.serialize("user-orders", byteArrayOutputStream.toByteArray());
    final ProducerRecord<String, String> record =
        new ProducerRecord<>("user-orders", "product", Objects.toString(product));
    Future<RecordMetadata> metadataFuture = kafkaProducer.send(record);
    RecordMetadata recordMetadata = metadataFuture.get();
    if (recordMetadata.offset() > 0) {
      kafkaProducer.close();
      System.out.println("Message Sent");
    } else
      System.err.println("Something went wrong");
  }
}
