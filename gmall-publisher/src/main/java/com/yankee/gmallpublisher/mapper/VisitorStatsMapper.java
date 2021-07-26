package com.yankee.gmallpublisher.mapper;

import com.yankee.gmallpublisher.bean.VisitorStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 访客接口主题Mapper
 * @date 2021/5/27 16:28
 */
@Mapper
public interface VisitorStatsMapper {
    @Select("select is_new, sum(uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(sv_ct) sv_ct, sum(uj_ct) uj_ct, sum(dur_sum) " +
            "dur_sum from visitor_stats where toYYYYMMDD(stt) = #{date} group by is_new")
    List<VisitorStats> selectVisitorStatsByNewFlag(int date);

    @Select("select sum(if(is_new = '1',  vs.uv_ct, 0)) new_uv, toHour(stt) hr, sum(uv_ct) uv_ct, sum(pv_ct) pv_ct, " +
            "sum(uj_ct) uj_ct from visitor_stats vs where toYYYYMMDD(stt) = #{date} group by toHour(stt) order by " +
            "toHour(stt);")
    List<VisitorStats> selectVisitorStatsByHr(int date);
}
