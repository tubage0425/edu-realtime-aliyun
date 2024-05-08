package com.atguigu.edu.service;

import com.atguigu.edu.beans.TrafficChAvgDurTimeBean;
import com.atguigu.edu.beans.TrafficKeywordsBean;

import java.util.List;

/**
 * ClassName: TrafficStatsService
 * Package: com.atguigu.edu.service
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 18:35
 * @Version: 1.0
 */
public interface TrafficStatsService {
    List<TrafficKeywordsBean> getKeywords(Integer date);
    List<TrafficChAvgDurTimeBean> getAvgDurTime(Integer date);
}
