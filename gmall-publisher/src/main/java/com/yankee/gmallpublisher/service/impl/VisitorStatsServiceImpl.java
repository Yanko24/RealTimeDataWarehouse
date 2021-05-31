package com.yankee.gmallpublisher.service.impl;

import com.yankee.gmallpublisher.bean.VisitorStats;
import com.yankee.gmallpublisher.mapper.VisitorStatsMapper;
import com.yankee.gmallpublisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 访客主题接口实现
 * @date 2021/5/27 16:37
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> selectVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> selectVisitorStatsByHr(int date) {
        return visitorStatsMapper.selectVisitorStatsByHr(date);
    }
}
