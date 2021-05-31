package com.yankee.gmallpublisher.mapper;

import com.yankee.gmallpublisher.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 按照地区统计Mapper
 * @date 2021/5/27 16:06
 */
public interface ProvinceStatsMapper {
    @Select("select province_name, sum(order_amount) order_amount from province_stats where toYYYYMMDD(stt) = #{date}" +
            " group by province_id, province_name")
    List<ProvinceStats> selectProvinceStats(int date);
}
