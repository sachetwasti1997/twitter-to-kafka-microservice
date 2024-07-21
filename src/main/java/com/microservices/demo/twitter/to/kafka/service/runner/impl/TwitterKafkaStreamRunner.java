package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Component
@ConditionalOnProperty(value = "${twitter-to-kafka-service.enable-mock-tweets}", havingValue = "false")
public class TwitterKafkaStreamRunner implements StreamRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData serviceConfigData;
    private final TwitterKafkaStatusListener statusListener;
    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData serviceConfigData,
                                    TwitterKafkaStatusListener statusListener) {
        this.serviceConfigData = serviceConfigData;
        this.statusListener = statusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(statusListener);
        addFilter();
    }

    @PreDestroy
    public void shut() {
        if (twitterStream != null) {
            LOGGER.info("Shutting down twitterStream Object");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords = serviceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery query = new FilterQuery(keywords);
        twitterStream.filter(query);
        LOGGER.info("Started streaming for the provided keywords");
    }
}
