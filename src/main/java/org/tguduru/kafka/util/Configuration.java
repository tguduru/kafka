package org.tguduru.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.producer.Producer;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * Defines utility methods for providing configurations for kafka producers & consumers.
 */
public class Configuration {
    private final static String KAFKA_CONFIG = "kafka-config.properties";
    private final static Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    /**
     * Returns the {@link ProducerConfig} for {@link Producer} implementations
     * @return {@link ProducerConfig}
     */
    public Properties getProducerProperties() {
        final Properties properties = new Properties();
        try {
            final URL url = ClassLoader.getSystemResource(KAFKA_CONFIG);
            properties.load(url.openStream());
        } catch (final IOException ex) {
            LOGGER.error("error while loading configurations : " + ex);
        }
        return properties;
    }

    /**
     * @return
     */
    private static ProducerConfig getDefaultProducerConfig() {
        return null;
    }

    public static void main(final String[] args) throws IOException {
        final Configuration configuration = new Configuration();
        final Properties properties = configuration.getProducerProperties();
        System.out.println(properties);
    }
}
