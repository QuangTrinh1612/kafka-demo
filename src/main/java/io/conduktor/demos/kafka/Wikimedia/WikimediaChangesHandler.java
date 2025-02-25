package io.conduktor.demos.kafka.Wikimedia;

import java.util.Objects;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;

public class WikimediaChangesHandler extends EventSourceListener {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    public WikimediaChangesHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = Objects.requireNonNull(kafkaProducer, "KafkaProducer cannot be null");
        this.topic = Objects.requireNonNull(topic, "Topic cannot be null");
    }

    @Override
    public void onOpen(EventSource eventSource, Response response) {
        logger.info("SSE stream opened for topic: {}", topic);
    }

    @Override
    public void onClosed(EventSource eventSource) {
        logger.info("SSE stream closed, shutting down Kafka producer for topic: {}", topic);
        try {
            kafkaProducer.flush(); // Ensure all queued messages are sent
            kafkaProducer.close();
            logger.info("Kafka producer closed successfully");
        } catch (Exception e) {
            logger.error("Failed to close Kafka producer cleanly", e);
        }
    }

    @Override
    public void onEvent(EventSource eventSource, String id, String type, String data) {
        if (data == null) {
            logger.warn("Received null data for event type: {}", type);
            return;
        }

        logger.info("Processing event type: {} with data: {}", type, data);

        // Asynchronous send with callback
        kafkaProducer.send(new ProducerRecord<>(topic, data), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("Failed to send message to Kafka topic {}: {}", topic, exception.getMessage(), exception);
                } else {
                    logger.debug("Message sent to topic {}, partition {}, offset {}", 
                            topic, metadata.partition(), metadata.offset());
                }
            }
        });
    }

    @Override
    public void onFailure(EventSource eventSource, Throwable t, Response response) {
        logger.error("Error in Stream Reading", t);
    }
}
