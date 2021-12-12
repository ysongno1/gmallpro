package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Options;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String data) {
        return dauMapper.selectDauTotal(data);
    }

    @Override
    public Map getDauHourTotal(String date) {
        List<Map> mapList = dauMapper.selectDauTotalHourMap(date);

        HashMap<String, Long> result = new HashMap<>();

        for (Map map : mapList) {
            result.put((String) map.get("LH"),(Long) map.get("CT"));
        }

        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvHoutTotal(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);

        HashMap<String, Double> result = new HashMap<>();
        for (Map map : mapList) {
            result.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        //TODO -----------------------------{}--------------------------------
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        //TODO -----------------------------bool------------------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //TODO -----------------------------query------------------------------
        searchSourceBuilder.query(boolQueryBuilder);
        //TODO -----------------------------term------------------------------
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", "2021-11-07");
        //TODO -----------------------------fiter------------------------------
        boolQueryBuilder.filter(termQueryBuilder);
        //TODO -----------------------------match------------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND);
        //TODO -----------------------------must------------------------------
        boolQueryBuilder.must(matchQueryBuilder);

        //TODO -----------------------------aggs------------------------------
        //  性别聚合
        //TODO -----------------------------terms------------------------------
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        //TODO -----------------------------terms------------------------------
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);


        //TODO -----------------------------from------------------------------
        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);

        //TODO -----------------------------must------------------------------
        searchSourceBuilder.size(size);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(GmallConstants.ES_DETAIL_INDEXNAME + "0625")
                .addType("_doc")
                .build();

        SearchResult searchResult = jestClient.execute(search);

        //获取命中条数
        Long total = searchResult.getTotal();

        //获取明细数据
        //创建存放明细数据的List集合
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            detail.add(source);
        }

        //TODO 3.获取性别数据
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> buckets = groupby_user_gender.getBuckets();
        //男生个数
        Long maleCount = 0L;
        for (TermsAggregation.Entry bucket : buckets) {
            if ("M".equals(bucket.getKey())) {
                maleCount += bucket.getCount();
            }
        }

        //男生占比
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        //女生占比
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        //创建存放性别占比的Option对象
        Options maleOpt = new Options("男", maleRatio);
        Options femaleOpt = new Options("女", femaleRatio);

        //创建存放性别占比的List集合
        ArrayList<Options> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        //创建性别占比的Stat对象
        Stat genderStat = new Stat(genderList, "用户性别占比");

        //TODO 4.获取年龄聚合组数据
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> groupby_user_ageBuckets = groupby_user_age.getBuckets();
        Long low20Count = 0L;
        Long up30Count = 0L;
        for (TermsAggregation.Entry bucket : groupby_user_ageBuckets) {
            //获取20岁以下的个数
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
            }

            //获取30岁及30岁以上的个数
            if (Integer.parseInt(bucket.getKey()) >= 30) {
                up30Count += bucket.getCount();
            }
        }

        //20岁以下年龄占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;
        //30岁及30岁以上年龄占比
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        //20岁到30岁年龄占比
        double up20WithLow30Ratio = Math.round((100 - low20Ratio - up30Ratio) * 10D) / 10D;

        //创建存放年龄占比的Options对象
        Options low20Opt = new Options("20岁以下", low20Ratio);
        Options up20Low30Opt = new Options("20岁到30岁", up20WithLow30Ratio);
        Options up30Opt = new Options("30岁及30岁以上", up30Ratio);
        //创建存放年龄占比的List集合
        ArrayList<Options> ageList = new ArrayList<>();
        ageList.add(low20Opt);
        ageList.add(up30Opt);
        ageList.add(up20Low30Opt);

        //存放年龄占比的Stat对象
        Stat ageStat = new Stat(ageList, "用户年龄占比");

        //创建存放Stat对象的List集合
        ArrayList<Stat> stat = new ArrayList<>();
        stat.add(genderStat);
        stat.add(ageStat);

        //创建存放最终结果的Map集合
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stat);
        result.put("detail", detail);
        return result;


    }
}
