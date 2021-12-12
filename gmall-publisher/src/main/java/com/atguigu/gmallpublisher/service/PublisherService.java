package com.atguigu.gmallpublisher.service;



import java.io.IOException;
import java.util.Map;

public interface PublisherService {

    //日活总数数据接口
    public Integer getDauTotal(String date);

    //分时统计数据接口
    public Map getDauHourTotal(String date);

    //新增交易额每日接口
    public Double getGmvTotal(String date);

    //新增交易额每时接口
    public Map getGmvHoutTotal(String date);

    //灵活查询数据接口
    public Map getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException;
}
