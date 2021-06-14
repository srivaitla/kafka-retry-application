package com.nord.kafka.retry.application.producer;

import com.nord.kafka.retry.application.util.RetryKafkaLogUtility;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RetryKafkaProducer {

    private static final Logger LOGGER = LogManager.getLogger(RetryKafkaProducer.class);

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    private final RetryKafkaLogUtility logUtility;

    @Autowired
    public RetryKafkaProducer(KafkaTemplate<String, SpecificRecordBase> kafkaTemplate,
                              RetryKafkaLogUtility logUtility) {
        this.kafkaTemplate = kafkaTemplate;
        this.logUtility = logUtility;
    }

    public void publishToTopic(ProducerRecord<String, SpecificRecordBase> record) {
        LOGGER.info("Producer ----- ----- Started : " + logUtility.logProducerRecord(record) + "\n");
        try {
            kafkaTemplate.send(record);
            LOGGER.info("Producer ----- ----- Completed : " + logUtility.logProducerRecord(record) + "\n\n\n");
        } catch (Exception ex) {
            LOGGER.info("Producer ----- ----- Exception : " + logUtility.logProducerRecord(record, ex) + "\n\n\n");
            throw ex;
        }
    }
}
