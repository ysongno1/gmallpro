package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Es03_JsonRead {
    public static void main(String[] args) throws IOException {

        //1.获取es创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2. 获取es配置
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);

        //3.获取es连接
        JestClient jestClient = jestClientFactory.getObject();

        Search search = new Search.Builder(
                "{\n" +
                        "  \"query\": {\n" +
                        "    \"bool\": {\n" +
                        "      \"filter\": {\n" +
                        "        \"term\": {\n" +
                        "          \"sex\": \"male\"\n" +
                        "        }\n" +
                        "      },\n" +
                        "      \"must\": [\n" +
                        "        {\n" +
                        "          \"match\": {\n" +
                        "            \"favo\": \"开车\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"aggs\": {\n" +
                        "    \"groupByClass\": {\n" +
                        "      \"terms\": {\n" +
                        "        \"field\": \"class_id\"\n" +
                        "      },\n" +
                        "      \"aggs\": {\n" +
                        "        \"groupByAge\": {\n" +
                        "          \"max\": {\n" +
                        "            \"field\": \"age\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}"
        )
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
