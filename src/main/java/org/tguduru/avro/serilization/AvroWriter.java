package org.tguduru.avro.serilization;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serializes the avro model
 * @author Guduru, Thirupathi Reddy
 * @modified 1/18/16
 */
public class AvroWriter {

    public ByteArrayOutputStream serialize(final SpecificRecord... specificRecords) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        final SpecificDatumWriter<SpecificRecord> datumWriter = new SpecificDatumWriter<>();
        for (final SpecificRecord specificRecord : specificRecords) {
            datumWriter.setSchema(specificRecord.getSchema());
            datumWriter.write(specificRecord, binaryEncoder);
        }
        binaryEncoder.flush();
        return byteArrayOutputStream;
    }
}
