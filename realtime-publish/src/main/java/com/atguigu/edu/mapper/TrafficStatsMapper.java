package com.atguigu.edu.mapper;

import com.atguigu.edu.beans.TrafficChAvgDurTimeBean;
import com.atguigu.edu.beans.TrafficKeywordsBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * ClassName: TrafficKeywordsMapper
 * Package: com.atguigu.edu.mapper
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 18:30
 * @Version: 1.0
 */
@Mapper
public interface TrafficStatsMapper {
    @Select("select\n" +
            "    keyword name,\n" +
            "    sum(keyword_count) value\n" +
            "    from dws_traffic_source_keyword_page_view_window partition par#{date}\n" +
            "group by keyword")
    List<TrafficKeywordsBean> selectKeywords(Integer date);

    // 各渠道平均浏览时长
    @Select("select\n" +
            "    ch,\n" +
            "    round(avg(dur_sum) / 1000, 2) avgDurTime\n" +
            "    from dws_traffic_vc_ch_ar_is_new_page_view_window partition par#{date}\n" +
            "group by ch")
    List<TrafficChAvgDurTimeBean> selectAvgDurTime(Integer date);

}
