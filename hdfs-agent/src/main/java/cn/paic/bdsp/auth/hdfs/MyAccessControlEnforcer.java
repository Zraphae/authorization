package cn.paic.bdsp.auth.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider.AccessControlEnforcer;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;


@Slf4j
class MyAccessControlEnforcer implements AccessControlEnforcer {
    private AccessControlEnforcer defaultEnforcer = null;

    public MyAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
        log.warn("==> RangerAccessControlEnforcer.RangerAccessControlEnforcer()");
        this.defaultEnforcer = defaultEnforcer;
    }

    public void checkPermission(String s, String s1, UserGroupInformation userGroupInformation, INodeAttributes[] iNodeAttributes, INode[] iNodes, byte[][] bytes, int i, String s2, int i1, boolean b, FsAction fsAction, FsAction fsAction1, FsAction fsAction2, FsAction fsAction3, boolean b1) throws AccessControlException {
        log.warn("---------------->my permission");
        log.warn("s:{}, s1:{}, userGroupInformation:{}, iNodeAttributes:{}, iNodes:{}, bytes:{}, i:{}, s2:{}, i1:{}, b:{}, fsAction:{}, fsAction1:{}, fsAction2:{}, fsAction3:{}, b1:{}", new Object[]{s, s1, userGroupInformation, iNodeAttributes, iNodes, bytes, i, s2, i1, b, fsAction, fsAction1, fsAction2, fsAction3, b1});
        this.defaultEnforcer.checkPermission(s, s1, userGroupInformation, iNodeAttributes, iNodes, bytes, i, s2, i1, b, fsAction, fsAction1, fsAction2, fsAction3, b1);
    }
}
