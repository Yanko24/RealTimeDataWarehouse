package com.yankee.gmallpublisher.service;

import com.yankee.gmallpublisher.bean.ProductStats;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 商品统计Service
 * @date 2021/5/24 14:03
 */
@Service
public interface ProductStatsService {
    // 读取某一天的总交易额
    public BigDecimal getGMV(int date);

    // 读取某个品牌某一天的交易额排名
    public List<ProductStats> getSumAmountByTmName(int date, int limit);

    List<ProductStats> getProductStatsByCategory3(int date, int limit);

    List<ProductStats> getProductStatsBySpu(int date, int limit);
}
