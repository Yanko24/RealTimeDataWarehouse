package com.yankee.gmallpublisher.mapper;

import com.yankee.gmallpublisher.bean.KeyWordStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 关键词统计Mapper
 * @date 2021/5/27 16:01
 */
@Mapper
public interface KeyWordStatsMapper {
    @Select("select word,sum(kws.word_count * multiIf(source = 'SEARCH', 10, source = 'ORDER', 3, source = 'CART', 2," +
            " source = 'CLICK', 1, 0)) word_count from key_word_stats kws where toYYYYMMDD(stt) = #{date} group by word " +
            "order by sum(kws.word_count) desc limit #{limit}")
    public List<KeyWordStats> selectKeyWordStats(@Param("date") int date, @Param("limit") int limit);
}
