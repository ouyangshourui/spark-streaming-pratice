package com.whz.platform.sparkKudu.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaUtil {
	private static KafkaUtil instance;
	  final int TIMEOUT = 100000;
	  final int BUFFERSIZE = 64 * 1024;

	  private KafkaUtil() {
	  }

	  public static synchronized KafkaUtil getInstance() {
	    if (instance == null) {
	      instance = new KafkaUtil();
	    }
	    return instance;
	  }
	
	public static void main(String[] args) {
		List<String> topics = new ArrayList<>();
	    topics.add("retailo2o-coll-user-0001-b2b2c-user");
	    Map<TopicAndPartition, Long> topicAndPartitionLongMap =
	    		KafkaUtil.getInstance().getLastOffset("172.172.177.70:9092,172.172.177.72:9092,172.172.177.73:9092", topics, "event_b2b2c_user_tidb");

	    for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMap.entrySet()) {
	     System.out.println(entry.getKey().topic() + "-"+ entry.getKey().partition() + ":" + entry.getValue());
	     if (entry.getValue() >
         topicAndPartitionLongMap.get(entry.getKey())) {
	    	 //矫正fromoffset为offset初始值0
	    	 entry.setValue(0L);
	     }
	    }

	}
	
	public Map<TopicAndPartition, Long> getLastOffset(String brokerList, List<String> topics,
		      String groupId) {

		    Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();

		    Map<TopicAndPartition, Broker> topicAndPartitionBrokerMap =
		    		KafkaUtil.getInstance().findLeader(brokerList, topics);

		    for (Map.Entry<TopicAndPartition, Broker> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap
		        .entrySet()) {
		      // get leader broker
		      Broker leaderBroker = topicAndPartitionBrokerEntry.getValue();

		      SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(),
		          TIMEOUT, BUFFERSIZE, groupId);

		      long readOffset = getTopicAndPartitionLastOffset(simpleConsumer,
		          topicAndPartitionBrokerEntry.getKey(), groupId);

		      topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);

		    }

		    return topicAndPartitionLongMap;

		  }
		
	private Map<TopicAndPartition, Broker> findLeader(String brokerList, List<String> topics) {
	    // get broker's url array
	    String[] brokerUrlArray = getBorkerUrlFromBrokerList(brokerList);
	    // get broker's port map
	    Map<String, Integer> brokerPortMap = getPortFromBrokerList(brokerList);

	    // create array list of TopicAndPartition
	    Map<TopicAndPartition, Broker> topicAndPartitionBrokerMap = Maps.newHashMap();

	    for (String broker : brokerUrlArray) {

	      SimpleConsumer consumer = null;
	      try {
	        // new instance of simple Consumer
	        consumer = new SimpleConsumer(broker, brokerPortMap.get(broker), TIMEOUT, BUFFERSIZE,
	            "leaderLookup" + new Date().getTime());

	        TopicMetadataRequest req = new TopicMetadataRequest(topics);

	        TopicMetadataResponse resp = consumer.send(req);

	        List<TopicMetadata> metaData = resp.topicsMetadata();

	        for (TopicMetadata item : metaData) {
	          for (PartitionMetadata part : item.partitionsMetadata()) {
	            TopicAndPartition topicAndPartition =
	                new TopicAndPartition(item.topic(), part.partitionId());
	            topicAndPartitionBrokerMap.put(topicAndPartition, part.leader());
	          }
	        }
	      } catch (Exception e) {
	        e.printStackTrace();
	      } finally {
	        if (consumer != null)
	          consumer.close();
	      }
	    }
	    return topicAndPartitionBrokerMap;
	  }
	private long getTopicAndPartitionLastOffset(SimpleConsumer consumer,
		      TopicAndPartition topicAndPartition, String clientName) {
		    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
		        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));

		    OffsetRequest request = new OffsetRequest(
		        requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
		        clientName);

		    OffsetResponse response = consumer.getOffsetsBefore(request);

		    if (response.hasError()) {
		      System.out
		          .println("Error fetching data Offset Data the Broker. Reason: "
		              + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
		      return 0;
		    }
		    long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
		    return offsets[0];
		  }
	private String[] getBorkerUrlFromBrokerList(String brokerlist) {
	    String[] brokers = brokerlist.split(",");
	    for (int i = 0; i < brokers.length; i++) {
	      brokers[i] = brokers[i].split(":")[0];
	    }
	    return brokers;
	  }
	
	 private Map<String, Integer> getPortFromBrokerList(String brokerlist) {
		    Map<String, Integer> map = new HashMap<String, Integer>();
		    String[] brokers = brokerlist.split(",");
		    for (String item : brokers) {
		      String[] itemArr = item.split(":");
		      if (itemArr.length > 1) {
		        map.put(itemArr[0], Integer.parseInt(itemArr[1]));
		      }
		    }
		    return map;
		  }
}
