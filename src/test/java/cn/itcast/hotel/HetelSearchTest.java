package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HetelSearchTest {
    private RestHighLevelClient client;
    @Autowired
    private IHotelService hotelService;
    @Test
    void testMatchAll() throws IOException {
        //1.准备request
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2.准备DSL
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        //3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析响应
        handleResponse(search);
    }

    @Test
    void matchQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.matchQuery("all","如家"));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析响应
        handleResponse(search);
    }

    @Test
    void Bool() throws IOException {
        //1. 准备request
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2. 准备DSL
        //2.1 准备BoolQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2 添加term
        boolQuery.must(QueryBuilders.termQuery("city","上海"));
        //2.3 添加range
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        searchRequest.source().query(boolQuery);
        //3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析响应
        handleResponse(search);
    }
    @Test
    void pageAndSort() throws IOException {
        int page= 2,size= 5;
        //1. 准备request
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2. 准备DSL
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        searchRequest.source().sort("price", SortOrder.ASC);
        searchRequest.source().from( (page - 1) * size ).size(size);
        //3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析响应
        handleResponse(search);
    }
    @Test
    void highLight() throws IOException {
        //1. 准备request
        SearchRequest searchRequest = new SearchRequest("hotel");
        //2. 准备DSL
        searchRequest.source().query(QueryBuilders.matchQuery("all","如家"));
        searchRequest.source().highlighter(new HighlightBuilder()
                .field("name")
                .requireFieldMatch(false));

        //3.发送请求
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        handleResponse(search);
    }
    //聚合
    @Test
    void aggs() throws IOException {
        //1. 准备request
        SearchRequest request = new SearchRequest("hotel");
        //2. 准备DSL
        request.source().size(0);
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg").field("brand").size(10));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms=aggregations.get("brandAgg");
        //4.1获取buckets
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        //4.2遍历
        for (Terms.Bucket bucket : buckets) {
            //4.3获取key
            String keyAsString = bucket.getKeyAsString();
            System.out.println(keyAsString);
        }
    }
    @Test
    void BulkRequest() throws IOException {
        //批量查询酒店数量
        List<Hotel> hotels=hotelService.list();
        //创建request
        BulkRequest request = new BulkRequest();
        //准备参数，添加多个新增的request
        for (Hotel hotel : hotels) {
            //转换为文档类型HotelDoc
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //创建新增文档的request对象
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }
        //发送请求
        this.client.bulk(request, RequestOptions.DEFAULT);
    }
    @Test
    void testSuggest() throws IOException {
        //1. 准备request
        SearchRequest request = new SearchRequest("hotel");
        //2. 准备DSL
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                .prefix("h")
                .skipDuplicates(true)
                .size(10)
        ));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestion = suggest.getSuggestion("suggestions");
        System.out.println(suggest);
        List<CompletionSuggestion.Entry.Option> options = suggestion.getOptions();
        for (CompletionSuggestion.Entry.Option option : options) {
            String text = option.getText().toString();
            System.out.println(text);
        }
    }

    private void handleResponse(SearchResponse search) {
        //4.解析结果
        SearchHits searchHits = search.getHits();
        //5.查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        //6.遍历数组得到source
        for (SearchHit hit : hits) {
            //获取文档source
            String json = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)){
                HighlightField highlightField = highlightFields.get("name");
                String name = highlightField.getFragments()[0].string();
                //将获取到的高亮值覆盖原属性
                hotelDoc.setName(name);
            }

            System.out.println(hotelDoc);
        }
        //7.查询的总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到"+total+"条数据");
    }

    @BeforeEach
    void setClient(){
        this.client=new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.173.128:9200")
        ));
    }
    @AfterEach
    void tearDown() throws IOException {
        client.close();
    }
}
