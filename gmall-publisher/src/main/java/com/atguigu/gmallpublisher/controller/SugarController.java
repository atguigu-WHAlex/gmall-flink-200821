package com.atguigu.gmallpublisher.controller;

import com.atguigu.gmallpublisher.bean.ProductStats;
import com.atguigu.gmallpublisher.service.ProductService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

//@Controller  返回页面
@RestController // =  @Controller + @ResponseBody
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductService productService;

//    @RequestMapping("/gmv")

    /**
     * {
     * "status": 0,
     * "msg": "",
     * "data": 1201083.4767158988
     * }
     */
    @RequestMapping("/gmv")
    //@ResponseBody
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = now();
        }

        return "{" +
                "\"status\": 0," +
                "\"msg\": \"\"," +
                "\"data\": " + productService.getSumAmount(date) + "" +
                "}";
    }

    /**
     * @return {
     * "status": 0,
     * "data": {
     * "categories": [
     * "苹果",
     * "三星",
     * "华为",
     * "oppo",
     * "vivo",
     * "小米33"
     * ],
     * "series": [
     * {
     * "name": "手机品牌",
     * "data": [
     * 6120,
     * 8196,
     * 5262,
     * 7291,
     * 5735,
     * 6592
     * ]
     * }
     * ]
     * }
     * }
     */
    @RequestMapping("/trademark")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit", defaultValue = "10") int limit) {

        if (date == 0) {
            date = now();
        }

        List<ProductStats> gmvByTm = productService.getGmvByTm(date, limit);

        //创建集合用于存放品牌名称,销售额
        ArrayList<String> tmList = new ArrayList<>();
        ArrayList<BigDecimal> amountList = new ArrayList<>();

        //遍历gmvByTm,给集合赋值
        for (ProductStats productStats : gmvByTm) {
            tmList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }

        //反转数据集
        Collections.reverse(tmList);
        Collections.reverse(amountList);

        return "{" +
                "\"status\": 0, " +
                "  \"data\": { " +
                "    \"categories\": [ \"" +
                StringUtils.join(tmList, "\",\"")  //"苹果","三星","华为","oppo","vivo","小米33"
                +
                "    \"], " +
                "    \"series\": [ " +
                "     \t{ " +
                "        \"name\": \"品牌\", " +
                "        \"data\": [ " +
                StringUtils.join(amountList, ",")//6120,8196,5262,7291,5735,6592
                +
                "        ] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";
    }


    private int now() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(new Date()));
    }


}
