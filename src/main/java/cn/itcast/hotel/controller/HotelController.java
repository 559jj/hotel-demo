package cn.itcast.hotel.controller;


import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {
    //注意map的key要为brand、city、starName。即文档中的字段名。否则前端是空白
    @Autowired
    IHotelService iHotelService;
    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParam requestParam){
        return iHotelService.search(requestParam);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> getFilters(@RequestBody RequestParam param){
        return iHotelService.filters(param);
    }

    @GetMapping("/suggestion")
    public List<String> getSuggestions(@org.springframework.web.bind.annotation.RequestParam("key") String prefix){
        return iHotelService.getSuggesions(prefix);
    }
}
