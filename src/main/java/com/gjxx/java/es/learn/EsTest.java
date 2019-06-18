package com.gjxx.java.es.learn;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

/**
 * @ClassName EsTest
 * @Description elasticsearch 学习
 * @Author SXS
 * @Date 2019/6/18 16:01
 * @Version 1.0
 */
public class EsTest {

    private static final String HOSTNAME = "cdh2";
    private static final int PORT = 9200;
    private static final String SCHEME = "http";
    private static RestHighLevelClient client;

    /**
     * @Author SXS
     * @Description 初始化client
     * @Date 18:51 2019/6/18
     * @Param []
     * @return void
     */
    @Before
    public void init() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(HOSTNAME, PORT, SCHEME)
                )
        );
    }

    /**
     * @Author SXS
     * @Description 关闭client
     * @Date 18:52 2019/6/18
     * @Param []
     * @return void
     */
    @After
    public void close() throws IOException {
        client.close();
    }

    /**
     * @Author SXS
     * @Description Index API
     * @Date 18:51 2019/6/18
     * @Param []
     * @return void
     */
    @Test
    public void indexApi() {
        IndexRequest request = new IndexRequest("posts");
        request.id("1");
        String jsonString = "{" +
                "\"user\": \"kimchy\"," +
                "\"postDate\": \"2013-01-30\"," +
                "\"message\": \"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        System.out.println("成功");
    }

    /**
     * @Author SXS
     * @Description //TODO
     * @Date 18:56 2019/6/18
     * @Param []
     * @return void
     */
    @Test
    public void indexApi2() {
        IndexRequest request = new IndexRequest("posts")
                .id("1")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");
        System.out.println("成功");
    }

    /**
     * @Author SXS
     * @Description Get API
     * @Date 19:00 2019/6/18
     * @Param []
     * @return void
     */
    @Test
    public void getApi() throws IOException {
        GetRequest getRequest = new GetRequest("posts", "1");
        GetResponse result = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(result);
    }

}
