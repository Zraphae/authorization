package cn.paic.bdsp.auth.hdfs;

import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;

@Slf4j
public class MyHdfsAuthorizer extends INodeAttributeProvider {

    public MyHdfsAuthorizer() {
        log.warn("---------->struct");
    }

    public void start() {
        log.info("---------------->start");
    }

    public void stop() {
        log.warn("-------------------->stop");
    }

    public INodeAttributes getAttributes(String[] fullPath, INodeAttributes iNodeAttributes) {
        log.warn("==> RangerHdfsAuthorizer.getAttributes(" + Arrays.toString(fullPath) + ")");
        return iNodeAttributes;
    }

    public AccessControlEnforcer getExternalAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
        log.warn("==> RangerHdfsAuthorizer.getExternalAccessControlEnforcer()");
        MyAccessControlEnforcer rangerAce = new MyAccessControlEnforcer(defaultEnforcer);
        return rangerAce;
    }
}