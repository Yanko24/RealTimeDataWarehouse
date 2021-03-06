package com.yankee.gmallpublisher.service;

import com.yankee.gmallpublisher.bean.KeyWordStats;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 关键词统计接口
 * @date 2021/5/27 16:03
 */
@Service
public interface KeyWordStatsService {
    public List<KeyWordStats> selectKeyWordStats(int date, int limit);
}
