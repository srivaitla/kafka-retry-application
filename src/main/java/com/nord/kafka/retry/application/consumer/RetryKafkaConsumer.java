package com.nord.kafka.retry.application.consumer;

import com.nord.kafka.retry.application.producer.RetryKafkaProducer;
import com.nord.kafka.retry.application.util.RetryKafkaLogUtility;
import com.nord.kafka.retry.application.util.RetryKafkaUtility;
import com.nord.kafka.retry.dto.RetryRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class RetryKafkaConsumer {

    private static final Logger LOGGER = LogManager.getLogger(RetryKafkaConsumer.class);

    private static final String CONSUMER_NAME = "Retry-Consumer";

    @Autowired
    private RetryKafkaProducer producer;

    @Autowired
    private RetryKafkaUtility utility;

    @Autowired
    private RetryKafkaLogUtility logUtility;

    @Value("${kafka.topic.dlq}")
    private String dlqTopic;

    @Value("${kafka.retry.topic.head}")
    private String retryTopicHead;

    @Value("${kafka.retry.topic.tail}")
    private String retryTopicTail;

    @Value("${kafka.retry.time1}")
    private int retryWaitTimeInSec1;

    @Value("${kafka.retry.time2}")
    private int retryWaitTimeInSec2;

    @Value("${kafka.retry.time3}")
    private int retryWaitTimeInSec3;

    @KafkaListener(topics = "${kafka.retry.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeRetryTopic(ConsumerRecord<String, RetryRequest> record) {
        LOGGER.info(CONSUMER_NAME + " ----- ----- Started : " + logUtility.logConsumerRecord(record) + "\n");
        try {
            final int retryCount = utility.getRetryCount(record.headers());
            final long retryTime = utility.currentTime() + TimeUnit.SECONDS.toMillis(getRetryWaitTime(retryCount));
            final String retryTopicNext = getRetryTopic(retryCount);

            producer.publishToTopic(utility.consumerToProducerRecord(record, retryTopicNext, retryCount, retryTime));
        } catch (Exception ex) {
            LOGGER.error(CONSUMER_NAME + " ----- ----- InterruptedException : " + logUtility.logConsumerRecord(record, ex) + "\n\n\n");
        }
        LOGGER.info(CONSUMER_NAME + " ----- ----- Completed : " + logUtility.logConsumerRecord(record) + "\n\n\n");
    }

    private String getRetryTopic(int retryCount) {
        return retryCount > 3 ? dlqTopic : retryTopicHead + retryCount + retryTopicTail;
    }

    private int getRetryWaitTime(int retryCount) {
        switch (retryCount) {
            case 1:
                return retryWaitTimeInSec1;
            case 2:
                return retryWaitTimeInSec2;
            case 3:
                return retryWaitTimeInSec3;
            default:
                return 0;
        }
    }

}
