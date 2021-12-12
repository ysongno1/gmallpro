package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Es04_ApiRead  {
    public static void main(String[] args) throws IOException {

        //1。获取es创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.获取es配置
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);

        //3.获取es连接
        JestClient jestClient = jestClientFactory.getObject();

        //一层方法 一层对象

        //TODO -----------------------------{}--------------------------------
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //TODO -----------------------------bool------------------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //TODO -----------------------------query------------------------------
        searchSourceBuilder.query(boolQueryBuilder);
        //TODO -----------------------------term------------------------------
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "male");
        //TODO -----------------------------fiter------------------------------
        boolQueryBuilder.filter(termQueryBuilder);
        //TODO -----------------------------match------------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "开车");
        //TODO -----------------------------must------------------------------
        boolQueryBuilder.must(matchQueryBuilder);

        //TODO -----------------------------terms------------------------------
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByClass").field("class_id");
        //TODO -----------------------------max------------------------------
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");
        //TODO -----------------------------aggs------------------------------
        searchSourceBuilder.aggregation(termsAggregationBuilder.subAggregation(maxAggregationBuilder));

        //TODO -----------------------------from------------------------------
        searchSourceBuilder.from(0);

        //TODO -----------------------------must------------------------------
        searchSourceBuilder.size(10);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();

        SearchResult result = jestClient.execute(search);

//命中条数
        System.out.println("total:" + result.getTotal());

        //获取明细数据
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:" + hit.index);
            System.out.println("_type:" + hit.type);
            System.out.println("_id:" + hit.id);
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o + ":" + source.get(o));
            }
        }

        //获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        //获取班级聚合组数据
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:" + bucket.getKey());
            System.out.println("doc_count:" + bucket.getCount());

            //获取年龄聚合组数据
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:" + groupByAge.getMax());
        }



        //关闭连接
        jestClient.shutdownClient();

    }
}
