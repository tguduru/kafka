package org.kafka.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import kafka.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

/**
 * Defines utility methods for providing configurations for kafka producers & consumers.
 */
public class ConfigImpl {
    private final static String KAFKA_CONFIG = "src//main//resources//kafka-config.properties";
    private final static Logger LOGGER = Logger.getLogger(ConfigImpl.class);

    /**
     * Returns the {@link ProducerConfig} for {@link Producer} implementations
     * @return {@link ProducerConfig}
     */
    public static ProducerConfig getProducerConfig() {
        try {
            final Properties properties = new Properties();
            final File file = new File(KAFKA_CONFIG);
            final FileInputStream inputStream = new FileInputStream(file);
            properties.load(inputStream);
            final ProducerConfig producerConfig = new ProducerConfig(properties);
            return producerConfig;
        } catch (final IOException ex) {
            LOGGER.error("error while loading configurations : " + ex);
        }
        return getDefaultProducerConfig();
    }

    /**
     * @return
     */
    private static ProducerConfig getDefaultProducerConfig() {
        return null;
    }

    public static void main(final String[] args) throws IOException {
        final ProducerConfig config = getProducerConfig();
        System.out.println(config.props());
    }
}
