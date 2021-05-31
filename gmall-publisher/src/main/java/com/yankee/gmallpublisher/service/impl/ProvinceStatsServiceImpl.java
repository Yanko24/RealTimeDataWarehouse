package com.yankee.gmallpublisher.service.impl;

import com.yankee.gmallpublisher.bean.ProvinceStats;
import com.yankee.gmallpublisher.mapper.ProvinceStatsMapper;
import com.yankee.gmallpublisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 按照地区统计接口实现
 * @date 2021/5/27 16:09
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> selectProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
