package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.ArrayList;

public class Es02_BulkWrite {
    public static void main(String[] args) throws IOException {

        //1.获取es创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2。获取es配置
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(build);

        //3.获取es连接
        JestClient jestClient = jestClientFactory.getObject();


        Movie movie1 = new Movie("002", "战狼");
        Movie movie2 = new Movie("003", "长津湖");
        Movie movie3 = new Movie("004", "你好，李焕英");
        Movie movie4 = new Movie("005", "哪吒之魔童降世");

        Index index1 = new Index.Builder(movie1).index("movie_0625").type("_doc").id("1002").build();
        Index index2 = new Index.Builder(movie2).id("1003").build();
        Index index3 = new Index.Builder(movie3).id("1004").build();
        Index index4 = new Index.Builder(movie4).id("1005").build();

        ArrayList<Index> indexs = new ArrayList<>();
        indexs.add(index1);
        indexs.add(index2);
        indexs.add(index3);
        indexs.add(index4);


        Bulk bulk = new Bulk.Builder()
                .addAction(indexs)
                .defaultIndex("movie_0625")
                .defaultType("_doc")
                .build();

        jestClient.execute(bulk);


        //关闭连接
        jestClient.shutdownClient();


    }
}
