package com.yankee.gmallpublisher.service;

import com.yankee.gmallpublisher.bean.ProvinceStats;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 按照地区统计Service
 * @date 2021/5/27 16:09
 */
@Service
public interface ProvinceStatsService {
    List<ProvinceStats> selectProvinceStats(int date);
}
