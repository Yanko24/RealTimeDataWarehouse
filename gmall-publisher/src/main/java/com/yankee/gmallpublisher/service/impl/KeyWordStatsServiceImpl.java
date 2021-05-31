package com.yankee.gmallpublisher.service.impl;

import com.yankee.gmallpublisher.bean.KeyWordStats;
import com.yankee.gmallpublisher.mapper.KeyWordStatsMapper;
import com.yankee.gmallpublisher.service.KeyWordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 关键词统计接口实现
 * @date 2021/5/27 16:04
 */
@Service
public class KeyWordStatsServiceImpl implements KeyWordStatsService {
    @Autowired
    KeyWordStatsMapper keyWordStatsMapper;

    @Override
    public List<KeyWordStats> selectKeyWordStats(int date, int limit) {
        return keyWordStatsMapper.selectKeyWordStats(date, limit);
    }
}
