package com.yankee.gmallpublisher.mapper;

import com.yankee.gmallpublisher.bean.ProductStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 商品统计Mapper
 * @date 2021/5/24 13:58
 */
@Mapper
public interface ProductStatsMapper {
    // 查询GMV总数
    @Select("select sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt) = #{date}")
    BigDecimal getSumAmount(int date);

    // 查询按照品牌分组下的GMV
    @Select("select tm_name, sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt) = #{date} group " +
            "by tm_name having order_amount > 0 order by order_amount DESC limit #{limit}")
    List<ProductStats> getSumAmountByTmName(@Param("date") int date, @Param("limit") int limit);

    @Select("select category3_id, category3_name, sum(order_amount) order_amount from product_stats where toYYYYMMDD" +
            "(stt) = #{date} group by category3_id, category3_name having order_amount > 0 order by order_amount desc" +
            " limit #{limit}")
    List<ProductStats> getProductStatsByCategory3(@Param("date") int date, @Param("limit") int limit);

    @Select("select spu_id, spu_name, sum(order_amount) order_amount , sum(order_ct) order_ct from product_stats " +
            "where toYYYYMMDD(stt) = #{date} group by spu_id, spu_name having order_amount > 0 order by order_amount " +
            "desc limit #{limit}")
    List<ProductStats> getProductStatsBySpu(@Param("date") int date, @Param("limit") int limit);
}
