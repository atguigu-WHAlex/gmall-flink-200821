package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.bean.ProductStats;
import com.atguigu.gmallpublisher.mapper.ProductMapper;
import com.atguigu.gmallpublisher.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class ProductServiceImpl implements ProductService {

    @Autowired
    ProductMapper productMapper;

    @Override
    public BigDecimal getSumAmount(int date) {
        return productMapper.getSumAmount(date);
    }

    @Override
    public List<ProductStats> getGmvByTm(int date, int limit) {
        return productMapper.getGmvByTm(date, limit);
    }
}
