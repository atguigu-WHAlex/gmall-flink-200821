package com.atguigu.gmallpublisher.mapper;

import com.atguigu.gmallpublisher.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface ProductMapper {

    //1.查询GMV总数  select sum(order_amount) order_amount
    //from product_stats_200821
    //where toYYYYMMDD(stt) = 20210305;
    @Select("select sum(order_amount) order_amount from product_stats_200821 where toYYYYMMDD(stt) = #{date}")
    BigDecimal getSumAmount(int date);

    //2.查询按照品牌分组下的GMV
    @Select("select tm_name,sum(order_amount) order_amount " +
            "from product_stats_200821 " +
            "where toYYYYMMDD(stt) = #{date} " +
            "group by tm_name " +
            "having order_amount>0 " +
            "order by order_amount desc " +
            "limit #{limit}")
    List<ProductStats> getGmvByTm(@Param("date") int date, @Param("limit") int limit);


}
