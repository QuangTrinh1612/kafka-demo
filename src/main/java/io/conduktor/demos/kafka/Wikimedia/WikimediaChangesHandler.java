package io.conduktor.demos.kafka.Wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;

public class WikimediaChangesHandler extends EventSourceListener {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    public WikimediaChangesHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen(EventSource eventSource, Response response) {
    }

    @Override
    public void onClosed(EventSource eventSource) {
        producer.close();
    }

    @Override
    public void onEvent(EventSource eventSource, String id, String type, String data) {
        logger.info(data);
        producer.send(new ProducerRecord<String,String>(topic, data));
    }

    @Override
    public void onFailure(EventSource eventSource, Throwable t, Response response) {
        logger.error("Error in Stream Reading", t);
    }
}
