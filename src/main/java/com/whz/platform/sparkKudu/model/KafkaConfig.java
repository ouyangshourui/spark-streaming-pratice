package com.whz.platform.sparkKudu.model;

public class KafkaConfig {
	private String topic;
	private String url;
	private String groupId;
	
	public String getGroupId() {
		return groupId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public KafkaConfig clone(){
		KafkaConfig stu = null;  
        try{  
            stu = (KafkaConfig)super.clone();  
        }catch(CloneNotSupportedException e) {  
            e.printStackTrace();  
        }  
        return stu;  
	}
}
