package cn.itcast.hotel.mq;

import cn.itcast.hotel.constants.MqConstants;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HotelListener {
    @Autowired
    IHotelService hotelService;
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MqConstants.HOTEL_INSERT_QUEUE),
            exchange = @Exchange(name = MqConstants.HOTEL_EXCHANGE,type = ExchangeTypes.TOPIC),//交换机默认是DirectExchange路由交换机
            key = MqConstants.HOTEL_INSERT_KEY))
    public void listenHotelInsertOrUpdate(Long id){
        hotelService.insertById(id);
    }
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MqConstants.HOTEL_DELETE_QUEUE),
            exchange = @Exchange(name = MqConstants.HOTEL_EXCHANGE,type = ExchangeTypes.TOPIC),//交换机默认是DirectExchange路由交换机
            key = MqConstants.HOTEL_DELETE_KEY))
    public void listenHotelDelete(Long id){

        hotelService.deleteById(id);
    }
}
