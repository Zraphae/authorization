package cn.paic.bdsp.auth.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

@Slf4j
public class Test {

    @org.junit.Test
    public void test() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://zhaopengs-MacBook-Pro.local:9000"), new Configuration(), "root");
        boolean flag = fs.mkdirs(new Path("/leitao3"));
        log.info("===>{}", flag);
    }
}