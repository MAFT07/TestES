package com.mmz.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchClientTest {
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
    public void createIndex() throws Exception {
        //1.创建一个Settings对象,相当于一个配置信息，主要是配置集群的名字

        //3.使用client创建一个索引库
        client.admin().indices().prepareCreate("index_hello")
                //执行
                .get();
        client.close();

    }
    @Test
    public void setMapping() throws Exception {

        //创建一个mapping信息
//        {
//            "article":{
//            "properties":{
//                "id":{
//                    "type":"long",
//                            "store":"true"
//                },
//                "title":{
//                    "type":"text",
//                            "store":"true",
//                            "index":"true",
//                            "analyzer":"ik_smart"
//                },
//                "content":{
//                    "type":"text",
//                            "store":"true",
//                            "index":"true",
//                            "analyzer":"ik_smart"
//                }
//            }
//
//     }
        XContentBuilder  builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type","long")
                                .field("store",true)
                            .endObject()
                            .startObject("title")
                                .field("type","text")
                                .field("store",true)
                         //       .field("index","true")
                                .field("analyzer","ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type","text")
                                .field("store",true)
                           //     .field("index","true")
                                .field("analyzer","ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        client.admin().indices()
                .preparePutMapping("index_hello")
                .setType("article")
                .setSource(builder)
                .get();
        client.close();

    }
    @Test
    public void testAddDocument() throws Exception{
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",1)
                    .field("title","北方入秋速度加快，多地降温可达10度")
                    .field("content","阿联酋一架飞机在纽约坠毁")
                .endObject();
        client.prepareIndex()
                .setIndex("index_hello")
                .setType("article")
                .setId("1")
                .setSource(builder)
                .get();
    }

    @Test
    public void testAddDocmentByJackson() throws Exception{
        Article article = new Article();
        article.setId(2);
        article.setTitle("这是一个Title");
        article.setContent("这是一个Content");
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(article);
        System.out.println(jsonDocument);
        client.prepareIndex("index_hello","article").setSource(jsonDocument, XContentType.JSON).get();
        client.close();


    }
    @Test
    public void testAddDocmentByJackson2() throws Exception{
        for (int i = 3; i < 100; i++) {
            Article article = new Article();
            article.setId(i);
            article.setTitle("女护士路遇昏迷男子跪地抢救，救人职责更是本能"+i);
            article.setContent("江西变质营养餐事件已致24人就医 多名官员被调查"+i);
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument = objectMapper.writeValueAsString(article);
            System.out.println(jsonDocument);
            client.prepareIndex("index_hello","article",i + "")
                    .setSource(jsonDocument, XContentType.JSON)
                    .get();
        }

        client.close();


    }



}
