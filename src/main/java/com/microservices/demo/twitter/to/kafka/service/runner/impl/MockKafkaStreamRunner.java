package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.KafkaClientException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(value = "${twitter-to-kafka-service.enable-mock-tweets}", havingValue = "true", matchIfMissing = true)
public class MockKafkaStreamRunner implements StreamRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener statusListener;

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
                                 TwitterKafkaStatusListener statusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.statusListener = statusListener;
    }

    private static final Random RANDOM = new Random();
    private static final String[] TWEET_MESSAGE_LIST = new String[]{
            "Lorem ipsum dolor sit ametSed",
            "ut perspiciatis unde omnis iste natus",
            "error sit voluptatem accusantium",
            "doloremque laudantium, totam rem aperiam",
            "eaque ipsa quae ab illo inventore veritatis",
            "et quasi architecto beatae vitae dicta sunt",
            "explicabo. Nemo enim ipsam voluptatem quia",
            "voluptas sit aspernatur aut odit aut fugit",
            "sed quia consequuntur magni dolores eos qui",
            "ratione voluptatem sequi nesciunt. Neque porro",
            "quisquam est, qui dolorem ipsum quia dolor sit amet",
            "consectetur, adipisci velit, sed quia non numquam eius",
            "modi tempora incidunt ut labore et dolore magnam",
            "aliquam quaerat voluptatem. Ut enim ad minima veniam",
            "quis nostrum exercitationem ullam corporis suscipit",
            "laboriosam, nisi ut aliquid ex ea commodi consequatur?",
            "Quis autem vel eum iure reprehenderit qui in ea",
            "voluptate velit esse quam nihil molestiae consequatur",
            "vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?",
            "Lorem ipsum dolor sit amet, consectetur",
            "adipiscing elit, sed do eiusmod tempor",
            "incididunt ut labore et dolore magna aliqua.",
            "Ut enim ad minim veniam, quis nostrud",
            "exercitation ullamco laboris nisi ut",
            "aliquip ex ea commodo consequat.",
            "Duis aute irure dolor in reprehenderit",
            "in voluptate velit esse cillum dolore",
            "eu fugiat nulla pariatur. Excepteur",
            "sint occaecat cupidatat non proident",
            "sunt in culpa qui officia",
            "deserunt mollit anim id est laborum."
    };

    private static final String TWEET_AS_RAW_JSON = "{"+
            "\"created_at\":\"{0}\", "+
            "\"id\":\"{1}\", "+
            "\"text\": \"{2}\", "+
            "\"user\": {\"id\":\"{3}\"} "+
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    @Override
    public void start() throws TwitterException, IOException, URISyntaxException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength
            , long sleepMs) throws TwitterException, IOException, URISyntaxException {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedRawJson = getFormattedRawJson(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedRawJson);
                    LOGGER.info("Message {}", formattedRawJson);
                    statusListener.onStatus(status);
                    sleep(sleepMs);
                }
            }catch (TwitterException ex) {
                LOGGER.error("Error Creating twitter status");
            }
        });
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error on sleep while creating new status");
        }
    }

    private String getFormattedRawJson(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomisedTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };
        String tweet = TWEET_AS_RAW_JSON;
        for (int i=0; i< params.length; i++) {
            tweet = tweet.replace("{"+i+"}", params[i]);
        }
        return tweet;
    }

    private String getRandomisedTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweetText = new StringBuilder();
        int tweetLength = (maxTweetLength - minTweetLength + 1) + minTweetLength;
        for (int i=0; i<tweetLength; i++) {
            tweetText.append(TWEET_MESSAGE_LIST[RANDOM.nextInt(TWEET_MESSAGE_LIST.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweetText.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweetText.toString().trim();
    }
}
