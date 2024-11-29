package com.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.net.URI;
import java.util.concurrent.TimeUnit;


@Service
public class WikimediaChangesProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {
        String topic = "wikimedia_recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        try {
            // Create an EventHandler to handle Wikimedia stream events
            EventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);
            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
            EventSource eventSource = builder.build();

            // Start streaming events
            logger.info("Starting Wikimedia stream...");
            eventSource.start();

            // Keep the stream alive for 10 minutes
            TimeUnit.MINUTES.sleep(10);
        } catch (Exception e) {
            logger.error("Error in WikimediaChangesProducer: ", e);
        }
    }
}
