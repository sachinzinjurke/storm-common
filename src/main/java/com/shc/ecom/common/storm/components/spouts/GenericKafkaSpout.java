package com.shc.ecom.common.storm.components.spouts;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;

import backtype.storm.topology.TopologyBuilder;

/**
 * @author djohn0
 *
 */
public class GenericKafkaSpout extends GenericSpout implements Serializable{
	
	private static final long serialVersionUID = 1463625107925065464L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(GenericKafkaSpout.class.getName());
	
	@Autowired 
	@Qualifier("myProperties")
	protected ReloadableResourceBundleMessageSource myProperties;
	
	private String topicName;
	
	private ConsumerConnector consumer;
	
	public GenericKafkaSpout(){
	}
	
	public GenericKafkaSpout(String topicName, final String componentId){
		super(componentId);
		this.topicName = topicName;		
	}
	
	private long startMs = 0;	
	private long totalBatchPartitionOffset = 1;
	protected long currentConsumedPartitionOffset;
	private boolean isBatchStarted = false;
	
	@Override
	public void addToTopology(TopologyBuilder builder) {
	}

	private ConsumerConfig createConsumerConfig() {		
		Properties props = new Properties();		
		props.put("zookeeper.connect", myProperties.getMessage("zookeeper.host.name", null, Locale.US));
		props.put("group.id",  myProperties.getMessage("consumer.group.id", null, Locale.US));
		props.put("zookeeper.session.timeout.ms", myProperties.getMessage("zookeeper.session.timeout.ms", null, Locale.US));
		props.put("zookeeper.sync.time.ms", myProperties.getMessage("zookeeper.sync.time.ms", null, Locale.US));
		props.put("auto.commit.interval.ms", myProperties.getMessage("auto.commit.interval.ms", null, "10000", Locale.US));
		props.put("auto.offset.reset", myProperties.getMessage("auto.offset.reset", null, Locale.US));
		props.put("rebalance.max.retries", myProperties.getMessage("rebalance.max.retries", null, "3",Locale.US));
		props.put("rebalance.backoff.ms", myProperties.getMessage("rebalance.backoff.ms", null, "10000", Locale.US));
		props.put("consumer.timeout.ms", myProperties.getMessage("consumer.timeout.ms", null, "60000", Locale.US));
		props.put("fetch.message.max.bytes", myProperties.getMessage("fetch.message.max.bytes", null, "1048576", Locale.US));
		
		return new ConsumerConfig(props);		
	}
	
	
	public ConsumerIterator<byte[], byte[]> getKafkaConsumerIterator(){
		
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topicName, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		
		return consumerMap.get(topicName).get(0).iterator();
		
	}
	
	public boolean hasNextMsg(ConsumerIterator<byte[], byte[]> consumerItr){
		boolean hasNextMsg = false;
		try{
			if(consumerItr.hasNext()){
				hasNextMsg = true;
			}
		}catch(ConsumerTimeoutException consumerTimeoutException){
			hasNextMsg = false;
		}
		return hasNextMsg;
	}
	
	public void shutDownConsumer(){
		
		if (consumer != null) consumer.shutdown();
		
	}
	
	public long getPartitionLogSize(ConsumerIterator<byte[], byte[]> consumerItr){
		long logSize = 0;
		PartitionMetadata partitionMeta = null;
		List<String> broker = new ArrayList<>();
		SimpleConsumer consumer = null;
		try{
			int partitionNum = consumerItr.kafka$consumer$ConsumerIterator$$currentTopicInfo().partitionId();
			String brokerDetails = myProperties.getMessage("kafka.broker.list", null, Locale.US);
			StringTokenizer brokerToken = new StringTokenizer(brokerDetails, ",");
			while(brokerToken.hasMoreTokens())
				broker.add(brokerToken.nextToken());
			
			for(String brokerHost : broker){
				StringTokenizer brokerHostToken = new StringTokenizer(brokerHost, ":");
				if(brokerHostToken.countTokens() == 2)
					partitionMeta = findLeader(brokerHostToken.nextToken(), Integer.parseInt(brokerHostToken.nextToken()), this.topicName, partitionNum);
				
				if(partitionMeta != null)
					break;
			}
			
			String leadBroker = partitionMeta.leader().host();
			consumer = new SimpleConsumer(leadBroker, partitionMeta.leader().port(), 100000, 64 * 1024, "leaderLookup");
			TopicAndPartition topicAndPartition = new TopicAndPartition(this.topicName, partitionNum);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), "");
			kafka.javaapi.OffsetResponse response = consumer.getOffsetsBefore(request);
			long[] offsets = response.offsets(this.topicName, partitionNum);
			logSize = offsets[0];
			
		}catch(Throwable thw){
			LOGGER.error("Error while getting partiton log size", thw);
		}finally {
            if (consumer != null) consumer.close();
        }
		
		//In any case if failed to fetch the details from leader broker override the size with the fetch request offset available in the iterator
		if(logSize == 0)
			logSize = consumerItr.kafka$consumer$ConsumerIterator$$currentTopicInfo().getFetchOffset();
		
		return logSize;
	}
	
	private PartitionMetadata findLeader(String brokerHost, int brokerPort, String topicName, int partitionNum) {
        PartitionMetadata partitionMeta = null;
        SimpleConsumer consumer = null;
        
        try {
            consumer = new SimpleConsumer(brokerHost, brokerPort, 100000, 64 * 1024, "leaderLookup");
            List<String> topics = Collections.singletonList(topicName);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

            List<TopicMetadata> metaData = resp.topicsMetadata();
            leader:
            for (TopicMetadata item : metaData) {
                for (PartitionMetadata meta : item.partitionsMetadata()) {
                    if (meta.partitionId() == partitionNum) {
                        partitionMeta = meta;
                        break leader;
                    }
                }
            }
        } catch (Exception e) {
        	LOGGER.error("Error communicating with Broker [" + brokerHost + "] to find Leader for [" + brokerPort + ", " + partitionNum + "]", e);
        } finally {
            if (consumer != null) consumer.close();
        }
    
        return partitionMeta;
    }
	
	public String fetchDelayedMsg(long delayedRetryWaitTime, ConsumerIterator<byte[], byte[]> consumerItr, String componentName){

		String delayedRetryMsg = null;
		long interval = System.currentTimeMillis() - startMs;

		if ((startMs == 0 && (currentConsumedPartitionOffset < totalBatchPartitionOffset)) || (startMs > 0 && interval >= delayedRetryWaitTime)
				&& (currentConsumedPartitionOffset < totalBatchPartitionOffset)) {
			if (hasNextMsg(consumerItr)) {
				if (!isBatchStarted) {
					totalBatchPartitionOffset = getPartitionLogSize(consumerItr);
					LOGGER.info("totalBatchPartitionOffset ::" + totalBatchPartitionOffset + ", for partition :: "
							+ consumerItr.kafka$consumer$ConsumerIterator$$currentTopicInfo().partitionId()
							+ " Thread :: " + Thread.currentThread().getName());
					isBatchStarted = true;
				}
				delayedRetryMsg = new String(consumerItr.next().message());
				currentConsumedPartitionOffset = consumerItr.kafka$consumer$ConsumerIterator$$consumedOffset();
				
				LOGGER.info("Fetched Msg Offset for component :: "+componentName +" , Offset :: "
						+ consumerItr.kafka$consumer$ConsumerIterator$$consumedOffset() + ", for partition :: "
						+ consumerItr.kafka$consumer$ConsumerIterator$$currentTopicInfo().partitionId()
						+ " Thread ::" + Thread.currentThread().getName());

			} else {
				isBatchStarted = false;
				startMs = System.currentTimeMillis();
				currentConsumedPartitionOffset = 0;
				totalBatchPartitionOffset = 1;
				if(LOGGER.isDebugEnabled()){
					LOGGER.debug("Recheck Interval :: "+ DurationFormatUtils.formatDuration(delayedRetryWaitTime, "HH:mm:ss SSSS"));
					LOGGER.debug("Current recheck Finished at :: " + getCurrentTime("yyyy-MM-dd HH:mm:ss SSSS"));
				}
			}
		} else {
			if (isBatchStarted) {
				isBatchStarted = false;
				startMs = System.currentTimeMillis();
				currentConsumedPartitionOffset = 0;
				totalBatchPartitionOffset = 1;
				if(LOGGER.isDebugEnabled()){
					LOGGER.debug("Recheck Interval :: "+ DurationFormatUtils.formatDuration(delayedRetryWaitTime, "HH:mm:ss SSSS"));
					LOGGER.debug("Current recheck Finished at :: " + getCurrentTime("yyyy-MM-dd HH:mm:ss SSSS"));
				}
			}
		}
		return delayedRetryMsg;
	}
	
	private String getCurrentTime(String formatter){
		DateFormat dateFormat = new SimpleDateFormat(formatter);
		Date date = new Date();
		return dateFormat.format(date);
	}

}
