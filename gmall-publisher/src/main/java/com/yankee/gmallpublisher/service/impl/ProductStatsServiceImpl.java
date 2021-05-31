package com.yankee.gmallpublisher.service.impl;

import com.yankee.gmallpublisher.bean.ProductStats;
import com.yankee.gmallpublisher.mapper.ProductStatsMapper;
import com.yankee.gmallpublisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 商品统计ServiceImpl
 * @date 2021/5/24 14:04
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getSumAmount(date);
    }

    @Override
    public List<ProductStats> getSumAmountByTmName(int date, int limit) {
        return productStatsMapper.getSumAmountByTmName(date, limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsByCategory3(date, limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySpu(int date, int limit) {
        return productStatsMapper.getProductStatsBySpu(date, limit);
    }
}
