package com.gjxx.java.es.learn;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @ClassName Demo
 * @Description TODO
 * @Author SXS
 * @Date 2019/6/18 14:56
 * @Version 1.0
 */
public class Demo {

    private String host = "cdh2";
    private int port = 9200;
    private String schema = "http";
    private int connectTimeOut = 1000;
    private int scoketTimeOut = 30000;
    private int connectionRequestTimeOut = 500;
    private int maxConnectNum = 100;
    private int maxConnectPerRoute = 100;
    private HttpHost httpHost;
    private boolean uniqueConnectTimeConfig = true;
    private boolean uniqueConnectNumConfig = true;
    private RestClientBuilder builder;
    private RestHighLevelClient client;




}
