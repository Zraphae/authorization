package cn.paic.bdsp.auth.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;

import java.util.List;

@Slf4j
public class AccessControlClientTest {

    public static void main(String[] args) throws Throwable {
        Configuration configuration = HBaseConfiguration.create();
        String[] group = {"test1"};
        User test1 = User.createUserForTesting(configuration, "zhaopeng", group);
        Connection connection = ConnectionFactory.createConnection(configuration, test1);
//        Connection connection = ConnectionFactory.createConnection(configuration);

//        AccessControlClient.grant(connection, TableName.valueOf("t1"),"test3", null, null, Permission.Action.READ);
        List<UserPermission> userPermissions = AccessControlClient.getUserPermissions(connection, ".*t*.");

        userPermissions.stream().forEach(userPermission -> log.info("==>userPermission: {}", userPermission));
    }
}