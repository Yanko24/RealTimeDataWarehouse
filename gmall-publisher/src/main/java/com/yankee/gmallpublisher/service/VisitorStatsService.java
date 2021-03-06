package com.yankee.gmallpublisher.service;

import com.yankee.gmallpublisher.bean.VisitorStats;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 访客主题接口
 * @date 2021/5/27 16:37
 */
@Service
public interface VisitorStatsService {
    List<VisitorStats> selectVisitorStatsByNewFlag(int date);

    List<VisitorStats> selectVisitorStatsByHr(int date);
}
