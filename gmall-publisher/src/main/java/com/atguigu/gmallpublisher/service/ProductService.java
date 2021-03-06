package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

public interface ProductService {

    //1.查询GMV总数
    BigDecimal getSumAmount(int date);

    //2.查询按照品牌分组下的GMV
    List<ProductStats> getGmvByTm(int date, int limit);

}
