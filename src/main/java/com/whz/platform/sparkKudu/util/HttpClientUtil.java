package com.whz.platform.sparkKudu.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * HTTP客户端帮助类
 * @author gang.nie
 * @date 2015-04-14 14:56:07  */
public class HttpClientUtil {
	private static final Logger LOG = LoggerFactory.getLogger(HttpClientUtil.class);
	public static final String CHAR_SET = "UTF-8";

	private static CloseableHttpClient httpClient;
	private static int socketTimeout = 30000;
	private static int connectTimeout = 30000;
	private static int connectionRequestTimeout = 30000;
	// 配置总体最大连接池（maxConnTotal）和单个路由连接最大数（maxConnPerRoute），默认是(20，2)
	private static int maxConnTotal = 200; // 最大不要超过1000
	private static int maxConnPerRoute = 100;// 实际的单个连接池大小，如tps定为50，那就配置50

	static {
		RequestConfig config = RequestConfig.custom()
				.setSocketTimeout(socketTimeout)
				.setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(connectionRequestTimeout).build();
		httpClient = HttpClients.custom().setDefaultRequestConfig(config)
				.setMaxConnTotal(maxConnTotal)
				.setMaxConnPerRoute(maxConnPerRoute).build();
	}

	public static CloseableHttpClient getClient() {
		return httpClient;
	}

	
	public static String get(String url) throws IOException {
		return get(url, null, null);
	}

	public static String get(String url, Map<String, String> map) throws IOException {
		return get(url, map, null);
	}

	public static String get(String url, String charset) throws IOException {
		return get(url, null, charset);
	}

	public static String get(String url, Map<String, String> paramsMap, String charset) throws IOException {
		if ( StringUtils.isBlank(url) ) {
			return null;
		}
		charset = (charset == null ? CHAR_SET : charset);
		if (null != paramsMap && !paramsMap.isEmpty()) {
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			for (Map.Entry<String, String> map : paramsMap.entrySet()) {
				params.add(new BasicNameValuePair(map.getKey(), map.getValue()));
			}
			//GET方式URL参数编码
			String querystring = URLEncodedUtils.format(params, charset);
			if(StringUtils.contains(url, "?")) {
				url += "&" + querystring;
			} else {
				url += "?" + querystring;
			}
		}
		HttpGet httpGet = new HttpGet(url);
		httpGet.addHeader("Accept-Encoding", "*");
		CloseableHttpResponse response = null;
		response = getClient().execute(httpGet);
		// 状态不为200的异常处理。
		return EntityUtils.toString(response.getEntity(), charset);
	}
	
	
	/**
	 * 提供返回json结果的get请求(ESG专用)
	 * @param url
	 * @param charset
	 * @return
	 * @throws IOException   */
	public static String getResponseJson(String url, Map<String, String> map) throws IOException {
		return getResponseJson(url, map, null);
	}

	public static String getResponseJson(String url, Map<String, String> paramsMap, String charset) throws IOException {
		if ( StringUtils.isBlank(url) ) {
			return null;
		}
		charset = (charset == null ? CHAR_SET : charset);
		if (null != paramsMap && !paramsMap.isEmpty()) {
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			for (Map.Entry<String, String> map : paramsMap.entrySet()) {
				params.add(new BasicNameValuePair(map.getKey(), map.getValue()));
			}
			//GET方式URL参数编码
			String querystring = URLEncodedUtils.format(params, charset);
			if(StringUtils.contains(url, "?")) {
				url += "&" + querystring;
			} else {
				url += "?" + querystring;
			}
		}
		HttpGet httpGet = new HttpGet(url);
		httpGet.addHeader("Accept-Encoding", "*");
		httpGet.addHeader("Accept", "application/json;charset=UTF-8");
		CloseableHttpResponse response = null;
		response = getClient().execute(httpGet);
		
		// 状态不为200的异常处理。
		return EntityUtils.toString(response.getEntity(), charset);
	}

	public static String delete(String url, Map<String, String> paramsMap, String charset) throws IOException {
		if ( StringUtils.isBlank(url) ) {
			return null;
		}
		charset = (charset == null ? CHAR_SET : charset);
		if (null != paramsMap && !paramsMap.isEmpty()) {
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			for (Map.Entry<String, String> map : paramsMap.entrySet()) {
				params.add(new BasicNameValuePair(map.getKey(), map.getValue()));
			}
			//GET方式URL参数编码
			String querystring = URLEncodedUtils.format(params, charset);
			if(StringUtils.contains(url, "?")) {
				url += "&" + querystring;
			} else {
				url += "?" + querystring;
			}
		}
		HttpDelete httpDelete = new HttpDelete(url);
		httpDelete.addHeader("Accept-Encoding", "*");
		CloseableHttpResponse response = getClient().execute(httpDelete);
		// 状态不为200的异常处理。
		return EntityUtils.toString(response.getEntity(), charset);
	}

	
	
	
	public static String post(String url, String request) throws ClientProtocolException, IOException {
		return post(url, request, null);
	}

	public static String post(String url, String request, String charset) throws ClientProtocolException, IOException {
		if ( StringUtils.isBlank(url) ) {
			return null;
		}
		String res = null;
		CloseableHttpResponse response = null;
		charset = (charset == null ? CHAR_SET : charset);
		StringEntity entity = new StringEntity(request, charset);
		HttpPost httpPost = new HttpPost(url);
		httpPost.addHeader("Content-Type", "application/json;charset=utf-8");
		httpPost.addHeader("Accept-Encoding", "*");
		httpPost.setEntity(entity);
		response = getClient().execute(httpPost);
		res = EntityUtils.toString(response.getEntity());
			// 状态不为200的异常处理。
		return res;
	}

	public static String post(String url, Map<String, String> map) throws ParseException, IOException {
		return post(url, map, null);
	}

	public static String post(String url, Map<String, String> paramsMap, String charset) throws ParseException, IOException {
		if ( StringUtils.isBlank(url) ) {
			return null;
		}
		String res = null;
		CloseableHttpResponse response = null;
		charset = (charset == null ? CHAR_SET : charset);
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		for (Map.Entry<String, String> map : paramsMap.entrySet()) {
			params.add(new BasicNameValuePair(map.getKey(), map.getValue()));
		}
		UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(params, charset);
		HttpPost httpPost = new HttpPost(url);
		httpPost.addHeader("Accept-Encoding", "*");
		httpPost.setEntity(formEntity);
		try {
			response = getClient().execute(httpPost);
		} catch (SocketTimeoutException e) {
			LOG.error("请求超时，1秒后将重试1次："+ url, e);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) { }
			response = getClient().execute(httpPost);
		}
		
		res = EntityUtils.toString(response.getEntity());
		// 状态不为200的异常处理。
		if (response.getStatusLine().getStatusCode() != 200) {
			throw new IOException(res);
		}
		return res;
	}
	
	
	/**
	 * 提供返回json结果的post请求 
	 * @param url
	 * @param map
	 * @param charset
	 * @return json
	 * @throws IOException  */
	public static String postResponseJson(String url, Map<String, String> map) {
		return postResponseJson(url, map, null);
	}
	
	/**
	 * Put方式提交
	 * @param url
	 * @param paramsMap
	 * @param charset
	 * @return response.getEntity()  */
	public static String postResponseJson(String url, Map<String, String> paramsMap, String charset) {
		if ( StringUtils.isBlank(url) ) {
			return null;
		}
		String res = null;
		CloseableHttpResponse response = null;
		try {
			charset = (charset == null ? CHAR_SET : charset);
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			for (Map.Entry<String, String> map : paramsMap.entrySet()) {
				params.add(new BasicNameValuePair(map.getKey(), map.getValue()));
			}
			UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(params, charset);
			
			//解决url中的一些特殊字符
			URI uri = null;
			HttpPost httpPost = null;
			try {
				URL url2 = new URL(url);
				uri = new URI(url2.getProtocol(), url2.getUserInfo(), url2.getHost(), url2.getPort(), url2.getPath(), url2.getQuery(), null);
			} catch (URISyntaxException e) {
				LOG.error("URL中存在非法编码："+ url, e);
				uri = null;
			}
			if(uri != null) {
				httpPost = new HttpPost( uri );
			} else {
				httpPost = new HttpPost( url );
			}
			httpPost.addHeader("Accept-Encoding", "*");
			httpPost.addHeader("Accept", "application/json;charset=UTF-8");
			httpPost.setEntity(formEntity);
			try {
				response = getClient().execute(httpPost);
			} catch (SocketTimeoutException e) {
//				LOG.error("请求超时，1秒后将重试1次："+ url, e);
				LOG.error(e.getMessage()+ "请求超时，1秒后将重试1次："+ url);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) { }
				response = getClient().execute(httpPost);
			}
			
			res = EntityUtils.toString(response.getEntity());
			// 状态不为200的异常处理。
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new IOException(res);
			}
		} catch (IOException e) {
			LOG.error("postResponseJson方法报错: ", e);
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (IOException e) {
				}
			}
		}
		return res;
	}
	

	/**
	 * Put方式提交
	 * @param url
	 * @param paramsMap
	 * @param charset
	 * @return response.getEntity()  */
	public static String put(String url, Map<String, String> paramsMap, String charset) {
		if ( StringUtils.isBlank(url) ) {
			return null;
		}
		String res = null;
		CloseableHttpResponse response = null;
		try {
			charset = (charset == null ? CHAR_SET : charset);
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			for (Map.Entry<String, String> map : paramsMap.entrySet()) {
				params.add(new BasicNameValuePair(map.getKey(), map.getValue()));
			}
			UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(params, charset);
			HttpPut httpPut = new HttpPut(url);
			httpPut.addHeader("Accept-Encoding", "*");
			httpPut.setEntity(formEntity);
			try {
				response = getClient().execute(httpPut);
			} catch (SocketTimeoutException e) {
				LOG.error("请求超时，1秒后将重试1次："+ url, e);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) { }
				response = getClient().execute(httpPut);
			}
			
			res = EntityUtils.toString(response.getEntity());
			// 状态不为200的异常处理。
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new IOException(res);
			}
		} catch (IOException e) {
			LOG.error("HTTP put(), IOException: ", e);
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (IOException e) {
				}
			}
		}
		return res;
	}

	
	/**
	 * Titel 提交HTTP请求，获得响应(application/x-www-form-urlencoded) Description
	 * 使用NameValuePair封装参数，适用于下载文件。
	 * @param serviceURI : 接口地址
	 * @param timeOut : 超时时间
	 * @param params : 请求参数
	 * @param charset : 参数编码
	 * @return CloseableHttpResponse  */
	public static CloseableHttpResponse submitPostHttpReq(final String serviceURI, final int timeOut, 
			final Map<String, String> pmap, final String charset) {
		CloseableHttpResponse response = null;
		LOG.info("即将发起HTTP请求！serviceURI：" + serviceURI);
		// 遍历封装参数
		List<NameValuePair> formParams = new ArrayList<NameValuePair>();
		for (String key : pmap.keySet()) {
			NameValuePair pair = new BasicNameValuePair(key, pmap.get(key));
			formParams.add(pair);
		}
		// 转换为from Entity
		UrlEncodedFormEntity fromEntity = null;
		try {
			fromEntity = new UrlEncodedFormEntity(formParams, CHAR_SET);
		} catch (UnsupportedEncodingException e) {
			LOG.error("创建UrlEncodedFormEntity异常，编码问题: " + e.getMessage());
			return null;
		}

		// 创建http post请求
		HttpPost httpPost = new HttpPost(serviceURI);
		httpPost.setEntity(fromEntity);
		// 设置请求和传输超时时间
//		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeOut).setConnectTimeout(timeOut).build();
//		httpPost.setConfig(requestConfig);

		// 提交请求、获取响应
		LOG.info("提交http请求！serviceURI：" + serviceURI);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		try {
			response = httpclient.execute(httpPost);
		} catch (SocketTimeoutException e) {
			LOG.error("请求超时，1秒后将重试1次："+ serviceURI, e);
			try {
				Thread.sleep(1000);
				response = httpclient.execute(httpPost);
			} catch (Exception e1) {
				LOG.error("超时后再次请求报错：", e);
				response = null;
			}
		} catch (IOException e) {
			LOG.error("httpclient.execute异常！" + e.getMessage());
			response = null;
		}
		LOG.info("提交http请求！serviceURI：" + serviceURI);
		return response;
	}


	public static int getSocketTimeout() {
		return socketTimeout;
	}

	public static void setSocketTimeout(int socketTimeout) {
		HttpClientUtil.socketTimeout = socketTimeout;
	}

	public static int getConnectTimeout() {
		return connectTimeout;
	}

	public static void setConnectTimeout(int connectTimeout) {
		HttpClientUtil.connectTimeout = connectTimeout;
	}

	public static int getConnectionRequestTimeout() {
		return connectionRequestTimeout;
	}

	public static void setConnectionRequestTimeout(int connectionRequestTimeout) {
		HttpClientUtil.connectionRequestTimeout = connectionRequestTimeout;
	}
	
	/**
	 * 请求josn串，返回josn对象
	 * */
	public static JSONObject doPost(String url, String jsonStr) {
		HttpPost post = new HttpPost(url);
		JSONObject response = null;
		try {
			StringEntity s = new StringEntity(jsonStr,"UTF-8");
			s.setContentType("application/json");// 发送json数据需要设置contentType
			post.setEntity(s);
			HttpResponse res = getClient().execute(post);
			if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				String result = EntityUtils.toString(res.getEntity());// 返回json格式：
				response = JSONObject.parseObject(result);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return response;
	}
}
