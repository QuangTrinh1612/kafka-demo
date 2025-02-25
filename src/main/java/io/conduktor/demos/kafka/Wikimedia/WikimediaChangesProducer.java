package io.conduktor.demos.kafka.Wikimedia;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSources;

public class WikimediaChangesProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) {
        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        OkHttpClient client = new OkHttpClient.Builder().build();
        Request request = new Request.Builder().url(url).build();
        WikimediaChangesHandler eventHandler = new WikimediaChangesHandler(producer, topic);

        EventSource eventSource = EventSources.createFactory(client).newEventSource(request, eventHandler);
        
        // Register a shutdown hook for clean exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            eventSource.cancel(); // Stop the SSE stream
            producer.close();     // Close the Kafka producer
        }));

        try {
            // Run for 10 minutes (blocking the main thread)
            logger.info("Producing events for 10 minutes...");
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            logger.error("Interrupted while running: " + e.getMessage());
            Thread.currentThread().interrupt(); // Restore interrupted status
        } finally {
            // Clean up resources
            eventSource.cancel();
            producer.close();
            logger.info("Producer and EventSource closed.");
        }
    }
}