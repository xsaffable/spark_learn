package com.gjxx.java.es.learn;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

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
    public void indexApi() throws IOException {
        IndexRequest request = new IndexRequest("posts");
        request.id("1");
        String jsonString = "{" +
                "\"user\": \"kimchy\"," +
                "\"postDate\": \"2013-01-30\"," +
                "\"message\": \"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        // 写入
        client.index(request, RequestOptions.DEFAULT);
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
    public void indexApi2() throws IOException {
        IndexRequest request = new IndexRequest("posts")
                .id("1")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");
        // 写入
        client.index(request, RequestOptions.DEFAULT);
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
        // 读取
        GetResponse result = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(result);
        System.out.println(result.getSource());
        System.out.println(result.getSource().get("message"));
    }

    /**
     * @Author SXS
     * @Description Exists API
     * @Date 9:21 2019/6/19
     * @Param []
     * @return void
     */
    @Test
    public void existsApi() throws IOException {
        GetRequest getRequest = new GetRequest("posts", "2");

        // 以下两个设置可以提高性能
        // 禁止取 _source
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        // 禁止取存储的变量
        getRequest.storedFields("_none");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * @Author SXS
     * @Description 异步 Exists API
     * @Date 9:30 2019/6/19
     * @Param []
     * @return void
     */
    @Test
    public void existsApi2() throws InterruptedException {
        GetRequest getRequest = new GetRequest("posts", "2");

        // 以下两个设置可以提高性能
        // 禁止取 _source
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        // 禁止取存储的变量
        getRequest.storedFields("_none");

        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean exists) {
                System.out.println("响应成功");
                System.out.println(exists);
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("响应失败");
                e.printStackTrace();
            }
        };

        client.existsAsync(getRequest, RequestOptions.DEFAULT, listener);

        // 给异步请求响应时间
        Thread.sleep(50);
    }

    /**
     * @Author SXS
     * @Description Update API
     * @Date 9:56 2019/6/19
     * @Param []
     * @return void
     */
    @Test
    public void updateApi() throws IOException {
        UpdateRequest request = new UpdateRequest("posts", "1")
                .doc("updated", new Date(),
                        "reason", "daily update",
                        "user", "affable");
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * @Author SXS
     * @Description Syn Update API
     * @Date 10:04 2019/6/19
     * @Param []
     * @return void
     */
    @Test
    public void updateApi2() throws InterruptedException {
        UpdateRequest request = new UpdateRequest("posts", "1")
                .doc("updated", new Date(),
                        "reason", "sec update",
                        "user", "affable");
        // 启动source搜索
        request.fetchSource(true);

        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse response) {
                System.out.println(response);
                updateResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };

        client.updateAsync(request, RequestOptions.DEFAULT, listener);

        // 给异步请求响应时间
        Thread.sleep(1000);
    }

    /**
     * @Author SXS
     * @Description update response
     * @Date 10:16 2019/6/19
     * @Param [response]
     * @return void
     */
    private void updateResponse(UpdateResponse response) {
        String index = response.getIndex();
        String id = response.getId();
        long version = response.getVersion();
        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("upsert");
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("update");
        } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
            System.out.println("deleted");
        } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
            System.out.println("no operation");
        } else {
            System.out.println("other");
        }
        System.out.println("index:" + index + ", id:" + id + ", version:" + version);

        GetResult result = response.getGetResult();
        if (result.isExists()) {
            Map<String, Object> source = result.sourceAsMap();
            source.forEach((k, v) -> System.out.println("key:value=" + k + ":" + v));
        }
    }

}
