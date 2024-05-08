package com.atguigu.edu.service.impl;

import com.atguigu.edu.beans.TrafficChAvgDurTimeBean;
import com.atguigu.edu.beans.TrafficKeywordsBean;
import com.atguigu.edu.mapper.TrafficStatsMapper;
import com.atguigu.edu.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ClassName: TrafficStatsServiceImpl
 * Package: com.atguigu.edu.service.impl
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 18:35
 * @Version: 1.0
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    TrafficStatsMapper trafficStatsMapper;
    @Override
    public List<TrafficKeywordsBean> getKeywords(Integer date) {
        return trafficStatsMapper.selectKeywords(date);
    }

    @Override
    public List<TrafficChAvgDurTimeBean> getAvgDurTime(Integer date) {
        return trafficStatsMapper.selectAvgDurTime(date);
    }
}
