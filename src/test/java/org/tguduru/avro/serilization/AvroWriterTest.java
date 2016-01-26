package org.tguduru.avro.serilization;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Test;
import org.tguduru.avro.schema.Product;
import org.tguduru.avro.schema.ProductType;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;

/**
 * @author Guduru, Thirupathi Reddy
 * @modified 1/18/16
 */
public class AvroWriterTest {

    @Test
    public void testSerialize() throws Exception {
        final Product product = Product.newBuilder().setName("Bose").setDescription("Music Head Phones").setPrice(100.0)
                .setProductType(ProductType.Electronics).build();
        final AvroWriter avroWriter = new AvroWriter();
        final ByteArrayOutputStream byteArrayOutputStream = avroWriter.serialize(product);

        final SpecificDatumReader<Product> datumReader = new SpecificDatumReader<>();
        datumReader.setSchema(Product.SCHEMA$);
        final BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(byteArrayOutputStream.toByteArray(),null);


        while(!binaryDecoder.isEnd()){
            final Product deserializeProduct = new Product();
            datumReader.read(deserializeProduct,binaryDecoder);
            System.out.println(deserializeProduct);
        }

    }
}