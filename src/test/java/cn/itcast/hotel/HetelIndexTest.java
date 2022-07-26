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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;
@SpringBootTest
public class HetelIndexTest {
    private RestHighLevelClient client;
    @Autowired
    IHotelService hotelService;
    @Test
    void createHotelIndex() throws IOException {
        //创建request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //准备请求参数:DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void deleteHotelIndex() throws IOException {
        //创建request对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("hotel");
        //发送请求
        client.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);
    }
    @Test
    void existHotelIndex() throws IOException {
        //创建request对象
        GetIndexRequest getIndexRequest = new GetIndexRequest("hotel");
        //发送请求
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.err.println(exists ? "索引库已存在":"索引库不存在");
    }
    @Test
    void addDocument() throws IOException {
        //根据id载数据库中查询酒店数据
        Hotel hotel = hotelService.getById(61083L);
        //转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //准备request对象
        IndexRequest indexRequest = new IndexRequest("hotel").id(hotel.getId().toString());
        //准备json文档
        indexRequest.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //准备请求
        client.index(indexRequest,RequestOptions.DEFAULT);
    }
    @Test
    void getDocument() throws IOException {
        //准备request
        GetRequest getRequest = new GetRequest("hotel","61083");
        //发送请求得到响应
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        //解析响应结果
        String json = response.getSourceAsString();
        //将json反序列化java对象
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void updateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("hotel","61083");
        //准备请求参数
        updateRequest.doc(
                "price","952",
                "starName","四钻"
        );
        client.update(updateRequest,RequestOptions.DEFAULT);
    }

    @Test
    void deleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("hotel","61083");
        client.delete(deleteRequest,RequestOptions.DEFAULT);
    }

    @Test
    void bulkRequest() throws IOException {
        //批量查询数据库中酒店数据
        List<Hotel> hotels = hotelService.list();
        //创建request
        BulkRequest bulkRequest = new BulkRequest();
        //准备参数，添加多个新增的request
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        client.bulk(bulkRequest,RequestOptions.DEFAULT);
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
