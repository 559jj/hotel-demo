package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    RestHighLevelClient client;
    @Override
    public PageResult search(RequestParam param) {
        try {
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");
            //2准备DSL
            //2.1query
            buildBasicQuery(param, request);
            //2.2分页
            Integer page = param.getPage();
            Integer size = param.getSize();
            request.source().from((page-1)*size).size(size);
            //2.3排序
            String location =param.getLocation();
            if (location!=null && !location.equals("")){
                request.source().sort(SortBuilders.geoDistanceSort("location",new GeoPoint(location))
                .order(SortOrder.ASC)
                .unit(DistanceUnit.KILOMETERS));
            }
            //3.发送请求
            SearchResponse search = client.search(request,RequestOptions.DEFAULT);
            //解析响应
            handleResponse(search);
            return handleResponse(search);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParam param) {
        try {
            //1. 准备request
            SearchRequest request = new SearchRequest("hotel");
            //2. 准备DSL
            //2.1query
            buildBasicQuery(param, request);
            //2.2 设置size
            request.source().size(0);
            //2.3 聚合
            request.source().aggregation(AggregationBuilders
                    .terms("brandAgg").field("brand").size(100));
            request.source().aggregation(AggregationBuilders
                    .terms("cityAgg").field("city").size(100));
            request.source().aggregation(AggregationBuilders
                    .terms("starNameAgg").field("starName.keyword").size(100));
            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析结果
            Map<String, List<String>> result=new HashMap<>();
            Aggregations aggregations = response.getAggregations();
            //4.1 根据聚合名称获取聚合结果
            List<String> brandList = getAggByName(aggregations,"brandAgg");
            List<String> cityList = getAggByName(aggregations,"cityAgg");
            List<String> starNameList = getAggByName(aggregations,"starNameAgg");
            //注意map的key要为brand、city、starName。即文档中的字段名。否则前端是空白
            result.put("brand",brandList);
            result.put("city",cityList);
            result.put("starName",starNameList);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggesions(String prefix) {
        try {
            //1. 准备request
            SearchRequest request = new SearchRequest("hotel");
            //2. 准备DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(prefix)
                            .skipDuplicates(true)
                            .size(10)
            ));
            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析结果
            Suggest suggest = response.getSuggest();
            //4.1根据补全查询名称，获取补全结果
            CompletionSuggestion suggestion = suggest.getSuggestion("suggestions");
            //4.2获取option
            List<CompletionSuggestion.Entry.Option> options = suggestion.getOptions();
            //4.3遍历
            List<String> list=new ArrayList<>();
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                list.add(text);
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            DeleteRequest request = new DeleteRequest("hotel",id.toString());
            client.delete(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            Hotel hotel = getById(id);
            HotelDoc hotelDoc = new HotelDoc(hotel);

            IndexRequest request = new IndexRequest("hotel").id(id.toString());
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            client.index(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getAggByName(Aggregations aggregations,String aggName) {
        Terms aggNameTerms= aggregations.get(aggName);
        //4.1获取buckets
        List<? extends Terms.Bucket> buckets = aggNameTerms.getBuckets();
        //4.2遍历
        List<String> aggNameList=new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            //4.3获取key
            String key = bucket.getKeyAsString();
            aggNameList.add(key);
        }
        return aggNameList;
    }

    private void buildBasicQuery(RequestParam param, SearchRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //根据关键字搜索 must部分
        String key = param.getKey();
        if (key==null || "".equals(key)){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("all",key));
        }
        //城市过滤
        if (param.getCity()!= null && !param.getCity().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("city", param.getCity()));
        }
        //品牌过滤
        if (param.getBrand()!= null && !param.getBrand().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("brand", param.getBrand()));
        }
        //星级过滤
        if (param.getStarName()!= null && !param.getStarName().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("starName.keyword", param.getStarName()));
        }
        //价格
        if (param.getMinPrice()!=null && param.getMaxPrice()!=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(param.getMinPrice()).lte(param.getMaxPrice()));
        }
        request.source().query(boolQuery);
    }

    private PageResult handleResponse(SearchResponse search) {
        //4.解析结果
        SearchHits searchHits = search.getHits();
        //5.查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        //6.遍历数组得到source
        List<HotelDoc> hotels=new ArrayList<>();
        for (SearchHit hit : hits) {
            //获取文档source
            String json = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length>0){
                Object sortValue=sortValues[0];
                hotelDoc.setDistance(sortValue);
            }

            hotels.add(hotelDoc);
        }
        //7.查询的总条数
        long total = searchHits.getTotalHits().value;
        return new PageResult(total,hotels);
    }
}
