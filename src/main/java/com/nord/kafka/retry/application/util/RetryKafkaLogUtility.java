package com.nord.kafka.retry.application.util;

import com.nord.kafka.retry.dto.RetryRequest;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RetryKafkaLogUtility {

    @Autowired
    private RetryKafkaUtility utility;

    public String logConsumerRecord(ConsumerRecord<String, RetryRequest> record) {
        return "ConsumerRecord[Topic=" + record.topic() + ", " + record.value()
                + ", Headers=" + utility.getHeadersAsString(record.headers()) + "]";
    }

    public String logConsumerRecord(ConsumerRecord<String, RetryRequest> record, Exception ex) {
        return logConsumerRecord(record) + ", Exception= " + ExceptionUtils.getStackTrace(ex);
    }

    public String logProducerRecord(ProducerRecord<String, SpecificRecordBase> record) {
        return "ProducerRecord[Topic=" + record.topic() + ", " + record.value()
                + ", Headers=" + utility.getHeadersAsString(record.headers()) + "]";
    }

    public String logProducerRecord(ProducerRecord<String, SpecificRecordBase> record, Exception ex) {
        return logProducerRecord(record) + ", Exception= " + ExceptionUtils.getStackTrace(ex);
    }

}
