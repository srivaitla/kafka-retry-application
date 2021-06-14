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

@Component
public class RetryKafkaConsumer2 {

    private static final Logger LOGGER = LogManager.getLogger(RetryKafkaConsumer2.class);

    private static final String CONSUMER_NAME = "Retry-Consumer-2";

    @Autowired
    private RetryKafkaProducer producer;

    @Autowired
    private RetryKafkaUtility utility;

    @Autowired
    private RetryKafkaLogUtility logUtility;

    @Value("${kafka.topic.dlq}")
    private String dlqTopic;

    @Value("${kafka.retry.time2}")
    private int retryTime;

    @KafkaListener(topics = "${kafka.retry.topic2}", groupId = "${spring.kafka.consumer.group-id2}")
    public void consumeRetryTopic2(ConsumerRecord<String, RetryRequest> record) {
        LOGGER.info(CONSUMER_NAME + " ----- ----- Started : " + logUtility.logConsumerRecord(record) + "\n");
        try {
            Thread.sleep(utility.getWaitingTime(record, CONSUMER_NAME));

            producer.publishToTopic(RetryKafkaUtility.convertBytePayloadToAVRORecord(record));
        } catch (InterruptedException ex) {
            LOGGER.error(CONSUMER_NAME + " ----- ----- InterruptedException : " + logUtility.logConsumerRecord(record, ex) + "\n\n\n");
        } catch (Exception ex) {
            LOGGER.error(CONSUMER_NAME + " ----- ----- Exception : " + logUtility.logConsumerRecord(record, ex) + "\n\n\n");
        }
        LOGGER.info(CONSUMER_NAME + " ----- ----- Completed : " + logUtility.logConsumerRecord(record) + "\n\n\n");
    }
}
