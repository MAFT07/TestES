package com.mmz.es;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class UpdateTest {

    private TransportClient client;
    @Before
    public void init() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name", "my-elasticsearch")
                .build();
        //2.创建一个client客户端对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
    }
    @Test
    public void testUpate() throws Exception{
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("id", 99 );
        map.put("title","update!!!!!");
        map.put("content", "!!!!!update");

        UpdateResponse updateResponse = client.prepareUpdate("index_hello", "article", "99")
                .setDoc(map).get();
        System.out.println("updateResponse索引名称:" + updateResponse.getIndex() + "\n updateResponse类型:" + updateResponse.getType()
                + "\n updateResponse文档ID:" + updateResponse.getId() + "\n当前实例updateResponse状态:" + updateResponse.status());

    }
}
