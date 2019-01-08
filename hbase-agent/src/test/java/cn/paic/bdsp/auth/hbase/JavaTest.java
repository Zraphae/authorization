package cn.paic.bdsp.auth.hbase;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class JavaTest {

    @Test
    public void testPattern(){

        List<String> list = Arrays.asList("test1", "test2");
        Pattern pattern = Pattern.compile("test*");
        Matcher matcher = pattern.matcher(list.toString());
        while(matcher.find()){
            log.info("");
        }


    }
}
