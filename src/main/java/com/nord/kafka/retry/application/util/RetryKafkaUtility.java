package com.nord.kafka.retry.application.util;

import com.nord.kafka.retry.application.consumer.RetryKafkaConsumer;
import com.nord.kafka.retry.dto.RetryRequest;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class RetryKafkaUtility {

    private static final Logger LOGGER = LogManager.getLogger(RetryKafkaConsumer.class);

    private final static String RETRY_COUNT = "retryCount";
    private final static String RETRY_TIME = "retryTime";

    @Autowired
    private RetryKafkaLogUtility logUtility;

    public String getHeadersAsString(Headers recordHeaders) {
        final StringBuilder headersBuilder = new StringBuilder().append('{');
        for (Header header : recordHeaders) {
            headersBuilder.append(header.key()).append('=').append(new String(header.value(), StandardCharsets.UTF_8)).append("; ");
        }
        headersBuilder.append('}');
        return headersBuilder.toString();
    }

    public String getHeadersAsString(List<Header> recordHeaders) {
        final StringBuilder headersBuilder = new StringBuilder().append('{');
        for (Header header : recordHeaders) {
            headersBuilder.append(header.key()).append('=').append(Arrays.toString(header.value())).append("; ");
        }
        headersBuilder.append('}');
        return headersBuilder.toString();
    }

    public ProducerRecord<String, SpecificRecordBase> consumerToProducerRecord(ConsumerRecord<String, RetryRequest> consumer,
                                                                               String topic, int retryCount, long retryTime) {
        final List<Header> headers = buildHeaderRecords(retryCount, retryTime);
        return new ProducerRecord<>(topic, null, consumer.key(), consumer.value(), headers);
    }

    private List<Header> buildHeaderRecords(int retryCount, long retryTime) {
        final Header retryCountHeader = new RecordHeader(RETRY_COUNT, String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8));
        final Header retryTimeHeader = new RecordHeader(RETRY_TIME, String.valueOf(retryTime).getBytes(StandardCharsets.UTF_8));

        final List<Header> producerHeaders = new ArrayList<>();
        producerHeaders.add(retryCountHeader);
        producerHeaders.add(retryTimeHeader);
        producerHeaders.add(new RecordHeader("ID", "200".getBytes(StandardCharsets.UTF_8)));
        producerHeaders.add(new RecordHeader("AppId", "RETRY".getBytes(StandardCharsets.UTF_8)));
        return producerHeaders;
    }

    public int getRetryCount(Headers headers) {
        int retryCount = 1;
        for (Header header : headers) {
            if (RETRY_COUNT.equals(header.key())) {
                retryCount = Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8)) + 1;
                break;
            }
        }
        return retryCount;
    }

    public long getWaitingTime(ConsumerRecord<String, RetryRequest> record, String consumer) {
        final long currentTime = currentTime();
        final long retryTime = getRetryTime(record.headers(), currentTime);
        final long waitingTime = retryTime - currentTime;
        LOGGER.info(consumer + "----- ----- Waiting for " + waitingTime + "ms, retryTime=" + retryTime + ", currentTime=" + currentTime + ", " + logUtility.logConsumerRecord(record) + "\n");
        return waitingTime > 0 ? waitingTime : 0;
    }

    private long getRetryTime(Headers headers, long currentTime) {
        for (Header header : headers) {
            if (RETRY_TIME.equals(header.key())) {
                return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        return currentTime;
    }

    public long currentTime() {
        return System.currentTimeMillis();
    }

    public static ProducerRecord<String, SpecificRecordBase> convertBytePayloadToAVRORecord(ConsumerRecord<String, RetryRequest> consumer) throws Exception {
        final SpecificRecordBase record = convertToAvroRecord(consumer.value());
        return new ProducerRecord<>(consumer.value().getConsumerRecord().getReplyTopic().toString(), null, consumer.key(), record,
                buildHeaders(consumer.value().getSourceRecord().getHeaders(), consumer.headers()));
    }

    public static SpecificRecordBase convertToAvroRecord(RetryRequest request)
            throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            ClassNotFoundException {
        final Class c = Class.forName(request.getPayloadRecord().getPayloadClassName().toString());
        final Method m = c.getDeclaredMethod("getClassSchema");
        final Schema schema = (Schema) m.invoke(c);
        return RetryKafkaUtility.deserialize(request.getPayloadRecord().getPayload().array(), schema);
    }

    public static SpecificRecordBase deserialize(byte[] arr, Schema schema) throws IOException {
        final SpecificDatumReader<SpecificRecordBase> reader = new SpecificDatumReader<>(schema);
        final Decoder decoder = DecoderFactory.get().binaryDecoder(arr, null);
        return reader.read(null, decoder);
    }

    public static byte[] serialize(SpecificRecordBase t) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        final DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(t.getSchema());
        writer.write(t, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    private static List<Header> buildHeaders(Map<CharSequence, CharSequence> sourceHeaders, Headers retryHeaders) {
        final List<Header> headers = new ArrayList<>();
        for (Map.Entry<CharSequence, CharSequence> header : sourceHeaders.entrySet()) {
            if (!RETRY_COUNT.equals(header.getKey().toString()) && !RETRY_TIME.equals(header.getKey().toString())) {
                headers.add(new RecordHeader(header.getKey().toString(), header.getValue().toString().getBytes(StandardCharsets.UTF_8)));
            }
        }
        for (Header header : retryHeaders) {
            if (RETRY_COUNT.equals(header.key()) || RETRY_TIME.equals(header.key())) {
                headers.add(new RecordHeader(header.key(), header.value()));
            }
        }
        return headers;
    }
}
