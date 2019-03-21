package com.whz.platform.sparkKudu.model;

import java.io.Serializable;

/**
 * jdbc连接驱动类
 * @author SHENGPENG745
 *
 */
public class JdbcModel implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String databaseKey;//数据库名
	private String driverClassName;//连接驱动类
	private String url;//连接地址
	private String username;//用户名
	private String password;//密码
	
	public JdbcModel(){}
	
	public JdbcModel(String databaseKey,String driverClassName,String url,String username,String password) {
		this.databaseKey = databaseKey;
		this.driverClassName = driverClassName;
		this.url = url;
		this.username = username;
		this.password = password;
	}
	
	public String getDriverClassName() {
		return driverClassName;
	}
	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	public String getDatabaseKey() {
		return databaseKey;
	}

	public void setDatabaseKey(String databaseKey) {
		this.databaseKey = databaseKey;
	}

}
