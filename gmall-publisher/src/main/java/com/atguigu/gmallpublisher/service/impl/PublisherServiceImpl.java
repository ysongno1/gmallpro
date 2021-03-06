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
        //?????? ??????
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
        //  ????????????
        //TODO -----------------------------terms------------------------------
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  ????????????
        //TODO -----------------------------terms------------------------------
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);


        //TODO -----------------------------from------------------------------
        // ??????= ?????????-1??? * ????????????
        searchSourceBuilder.from((startpage - 1) * size);

        //TODO -----------------------------must------------------------------
        searchSourceBuilder.size(size);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(GmallConstants.ES_DETAIL_INDEXNAME + "0625")
                .addType("_doc")
                .build();

        SearchResult searchResult = jestClient.execute(search);

        //??????????????????
        Long total = searchResult.getTotal();

        //??????????????????
        //???????????????????????????List??????
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            detail.add(source);
        }

        //TODO 3.??????????????????
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> buckets = groupby_user_gender.getBuckets();
        //????????????
        Long maleCount = 0L;
        for (TermsAggregation.Entry bucket : buckets) {
            if ("M".equals(bucket.getKey())) {
                maleCount += bucket.getCount();
            }
        }

        //????????????
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        //????????????
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        //???????????????????????????Option??????
        Options maleOpt = new Options("???", maleRatio);
        Options femaleOpt = new Options("???", femaleRatio);

        //???????????????????????????List??????
        ArrayList<Options> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        //?????????????????????Stat??????
        Stat genderStat = new Stat(genderList, "??????????????????");

        //TODO 4.???????????????????????????
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> groupby_user_ageBuckets = groupby_user_age.getBuckets();
        Long low20Count = 0L;
        Long up30Count = 0L;
        for (TermsAggregation.Entry bucket : groupby_user_ageBuckets) {
            //??????20??????????????????
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
            }

            //??????30??????30??????????????????
            if (Integer.parseInt(bucket.getKey()) >= 30) {
                up30Count += bucket.getCount();
            }
        }

        //20?????????????????????
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;
        //30??????30?????????????????????
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        //20??????30???????????????
        double up20WithLow30Ratio = Math.round((100 - low20Ratio - up30Ratio) * 10D) / 10D;

        //???????????????????????????Options??????
        Options low20Opt = new Options("20?????????", low20Ratio);
        Options up20Low30Opt = new Options("20??????30???", up20WithLow30Ratio);
        Options up30Opt = new Options("30??????30?????????", up30Ratio);
        //???????????????????????????List??????
        ArrayList<Options> ageList = new ArrayList<>();
        ageList.add(low20Opt);
        ageList.add(up30Opt);
        ageList.add(up20Low30Opt);

        //?????????????????????Stat??????
        Stat ageStat = new Stat(ageList, "??????????????????");

        //????????????Stat?????????List??????
        ArrayList<Stat> stat = new ArrayList<>();
        stat.add(genderStat);
        stat.add(ageStat);

        //???????????????????????????Map??????
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stat);
        result.put("detail", detail);
        return result;


    }
}
