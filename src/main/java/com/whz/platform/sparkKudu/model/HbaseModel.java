package com.whz.platform.sparkKudu.model;

import java.io.Serializable;

public class HbaseModel implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String zookeeperQuorum;
	private int clientPort;
	private String znodeParent;
	
	public HbaseModel(){}
	
	public HbaseModel(String zookeeperQuorum,int clientPort,String znodeParent){
		this.zookeeperQuorum = zookeeperQuorum;
		this.clientPort = clientPort;
		this.znodeParent = znodeParent;
	}
	
	public String getZookeeperQuorum() {
		return zookeeperQuorum;
	}
	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}
	public int getClientPort() {
		return clientPort;
	}
	public void setClientPort(int clientPort) {
		this.clientPort = clientPort;
	}

	public String getZnodeParent() {
		return znodeParent;
	}

	public void setZnodeParent(String znodeParent) {
		this.znodeParent = znodeParent;
	}

}
