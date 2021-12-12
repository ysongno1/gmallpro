package com.atguigu.write;


import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es01_SingleWrite {
    public static void main(String[] args) throws IOException {

        //1.获取es创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.获取es配置
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);

        //3.获取es连接
        JestClient jestClient = jestClientFactory.getObject();

        //写入一条数据
        Movie movie = new Movie("001", "请回答1988");
        Index index = new Index.Builder(movie)
                .index("movie_0625")
                .type("_doc")
                .id("1001")
                .build();

        jestClient.execute(index);


        //关闭连接
        jestClient.shutdownClient();


    }
}
