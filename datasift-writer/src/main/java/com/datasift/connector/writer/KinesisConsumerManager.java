package com.datasift.connector.writer;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.datasift.connector.writer.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;

/**
 * Created by ivaramme on 2/17/16.
 */
public class KinesisConsumerManager implements ConsumerManager {
    private static Logger log =
            LoggerFactory.getLogger(KinesisConsumerManager.class);

    /**
     * Metrics object for reporting.
     */
    private Metrics metrics;

    /**
     * Kafka topic for which to read items.
     */
    private String topic;

    /**
     * Partition ID to read from under broker.
     */
    private String partition;

    /**
     * Cache to store items read from Kinesis fetch requests, prior to processing.
     */
    private Queue<ConsumerData> dataItems = new ArrayDeque<ConsumerData>();

    /**
     * Configuration object containing Kafka & Zookeeper specific properties.
     */
    private Config config;

    /**
     * Offset of the message which was last processed and sent onward.
     */
    private String lastShardSequenceNumberProcessed = null;

    /**
     * Offset from which to continue after a reset.
     */
    private String initialShardSequenceNumber = null;

    private String readSequenceNumber = null;

    /**
     * Offset from which to perform next queue read.
     */
    private String readShardIterator = null;

    private AmazonKinesisClient client;

    public KinesisConsumerManager(final Config config, final Metrics metrics) {
        this.config = config;
        this.metrics = metrics;
        this.topic = config.kafka.topic;
        this.partition = "0";

        this.client = new AmazonKinesisClient();
    }

    @Override
    public ConsumerData readItem() {
        if(dataItems.size() > 0) {
            log.debug("Returning object from local queue");
            ConsumerData item = dataItems.remove();
            lastShardSequenceNumberProcessed = item.getShardSequenceNumber();
            log.info("Object: {}", item.getMessage());
            metrics.readItemFromQueue.mark();
            return item;
        }

        // TODO: fetch objects from kinesis
        metrics.readItemsFromKinesis.mark();
        metrics.readKafkaItemsFromConsumer.mark();

        if(lastShardSequenceNumberProcessed == null) { // first time
            lastShardSequenceNumberProcessed = fetchLastCheckpoint(topic); // update reference in case we have to reset
            if(lastShardSequenceNumberProcessed == null && readSequenceNumber != null) {
                lastShardSequenceNumberProcessed = readSequenceNumber;
            }
        }

        // Initially a good idea to do this here, but at this point, we don't know if all the items in the batch
        // have been processed or not, so we can't move forward.
        // if(null == readShardIterator || readShardIterator.isEmpty()) {
            readShardIterator = getShardIterator(topic, partition, lastShardSequenceNumberProcessed);
        // }

        int processed = 0;
        if(readShardIterator != null && !readShardIterator.isEmpty()) {
            GetRecordsResult results = getRecords(readShardIterator);
            if(null != results) {
                for (Record record : results.getRecords()) {
                    byte[] bytes = new byte[record.getData().limit()];
                    record.getData().get(bytes);

                    try {
                        String data = new String(bytes, "UTF-8");
                        ConsumerData item = new ConsumerData(record.getSequenceNumber(), data);
                        dataItems.add(item);
                        metrics.readItemFromKinesis.mark();
                        metrics.readKafkaItem.mark();
                    } catch (UnsupportedEncodingException e) {
                        log.error("Error parsing object from Kinesis: {}", e);
                        metrics.parsingError.mark();
                    }
                    processed++;
                }
                // Get next shard iterator, to avoid making a request
                readShardIterator = results.getNextShardIterator();
                if (null == readShardIterator) {
                    reshard(topic);
                }
            } else {
                readShardIterator = null; // force a retrieve of the shardIterator
            }
        }

        if(processed > 0) {
            ConsumerData item = dataItems.remove();
            lastShardSequenceNumberProcessed = item.getShardSequenceNumber();
            metrics.readItemFromQueue.mark();
            return item;
        }
        return null;
    }

    @Override
    public boolean commit() {
        log.info("Committing offset {} for topic {}", lastShardSequenceNumberProcessed, topic);
        initialShardSequenceNumber = lastShardSequenceNumberProcessed;
        persistCheckpoint(topic, lastShardSequenceNumberProcessed);
        return false;
    }

    @Override
    public boolean reset() {
        if (dataItems != null) {
            dataItems.clear();
            if (dataItems.isEmpty()) {
                log.debug("Consumer reset: Reverting next read offset from " + readSequenceNumber + " to " + initialShardSequenceNumber);
                readSequenceNumber = initialShardSequenceNumber;
                return true;
            }
        }

        return false;
    }

    @Override
    public void shutdown() {

    }

    private String getShardIterator(final String streamName, final String shardId, final String startFromSequenceNumber) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shardId);

        if(null != startFromSequenceNumber && !startFromSequenceNumber.isEmpty()) {
            getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER");
            getShardIteratorRequest.setStartingSequenceNumber(startFromSequenceNumber);
            log.info("Reading Kinesis from sequence number {}", startFromSequenceNumber);
        } else {
            getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
            log.info("Reading Kinesis from beginning");
            metrics.readKinesisFromStart.mark();
        }

        GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
        return getShardIteratorResult.getShardIterator();
    }

    private GetRecordsResult getRecords(final String fromShardIterator) {
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(fromShardIterator);
        getRecordsRequest.setLimit(30);

        GetRecordsResult result = client.getRecords(getRecordsRequest);
        return result;
    }

    private void reshard(final String streamName) {
        DescribeStreamRequest streamInfoRequest = new DescribeStreamRequest()
                                                    .withStreamName(streamName);
        DescribeStreamResult streamInfoResult = client.describeStream(streamInfoRequest);
        List<Shard> shards = streamInfoResult.getStreamDescription().getShards();
        log.debug("Retrieving shard data for stream {}", streamName);

        if(shards.size() > 0) {
            partition = shards.get(0).getShardId();
        } else {
            throw new RuntimeException("No shards are available to consume data from for stream " + streamName);
        }
    }

    private String fetchLastCheckpoint(String topic) {
        String value = "";
        try {
            value = new String(readAllBytes(get("checkpoint-"+topic+".txt")));
            log.info("Reading checkpoint from file: {}", value);
        } catch (IOException e) {
            log.error("Unable to read from checkpoint for topic {}, exception: {}.", topic, e);
        }
        if(value != null && !value.isEmpty())
            return value;

        return null;
    }

    private void persistCheckpoint(String topic, String sequenceNumber) {
        // TODO: persist externally as well.
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("checkpoint-"+topic+".txt"), "utf-8"))) {
            writer.write(sequenceNumber);
        } catch (IOException e) {
            log.error("Unable to log checkpoint {} to file, expection {}.", sequenceNumber, e);
        }
    }
}
