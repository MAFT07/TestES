package com.mmz.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class SearchIndex {
    private TransportClient client;
    @Before
    public void init() throws Exception{
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
    }

    private void search(QueryBuilder queryBuilder) throws Exception{
        //执行查询
        SearchResponse searchResponse =client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                //设置分页信息
                .setFrom(0)
                .setSize(5)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询记录总数："+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            //打印文档对象，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("----------文档的属性--------");
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));

        }
        //关闭client
        client.close();
    }
    @Test
    public void testSearchById() throws Exception{
        //创建client对象
        //创建一个查询对象
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("3","1");
        search(queryBuilder);
    }
    @Test
    public void testSearchByTerm() throws Exception{
        //创建一个QueryBuilder对象
        //参数1:要搜索的字段，参数2:要搜索的关键词
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title","北方");
        search(queryBuilder);
    }
    @Test
    public void testQueryStringQuery() throws Exception{
        //创建一个QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("昏迷男子")
                .defaultField("title");
        search(queryBuilder);
    }
    private void search(QueryBuilder queryBuilder,String highlightField) throws Exception{
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //高亮显示字段
        highlightBuilder.field(highlightField);
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");

        //执行查询
        SearchResponse searchResponse =client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                //设置分页信息
                .setFrom(0)
                .setSize(5)
                //设置高亮显示信息
                .highlighter(highlightBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        System.out.println("查询记录总数："+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();
            //打印文档对象，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("----------文档的属性--------");
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            System.out.println("**********高亮结果**********");
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println(highlightFields);
            System.out.println("!!!!!!!!!!高亮结果!!!!!!!!");
            HighlightField field = highlightFields.get(highlightField);
            Text[] fragments = field.getFragments();
            if(fragments!=null){
                String title = fragments[0].toString();
                System.out.println(title);
            }
        }
        //关闭client
        client.close();
    }
    @Test
    public void testQueryStringQuery2() throws Exception{
        //创建一个QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("昏迷男子")
                .defaultField("title");
        search(queryBuilder,"title");
    }
}
