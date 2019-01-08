package cn.paic.bdsp.auth.hbase;

import com.google.common.collect.MapMaker;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.BaseMasterAndRegionObserver;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService.Interface;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AuthResult;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

@Slf4j
@InterfaceAudience.LimitedPrivate({"Configuration"})
public class BDSPCoprocessor extends BaseMasterAndRegionObserver implements RegionServerObserver, Interface, CoprocessorService, EndpointObserver, BulkLoadObserver {
    private static final Log LOG = LogFactory.getLog(BDSPCoprocessor.class);
    private static final String CHECK_COVERING_PERM = "check_covering_perm";
    private static final String TAG_CHECK_PASSED = "tag_check_passed";
    private static final byte[] TRUE = Bytes.toBytes(true);
    boolean aclRegion = false;
    private RegionCoprocessorEnvironment regionEnv;
    private Map<InternalScanner, String> scannerOwners = (new MapMaker()).weakKeys().makeMap();
    private UserProvider userProvider;
    boolean authorizationEnabled;
    boolean cellFeaturesEnabled;
    boolean shouldCheckExecPermission;
    boolean compatibleEarlyTermination;
    private volatile boolean initialized = false;
    private volatile boolean aclTabAvailable = false;

    public BDSPCoprocessor() {
    }

    public static boolean isAuthorizationSupported(Configuration conf) {
        return conf.getBoolean("hbase.security.authorization", true);
    }

    public Region getRegion() {
        return this.regionEnv != null ? this.regionEnv.getRegion() : null;
    }


    AuthResult permissionGranted(String request, User user, Action permRequest, RegionCoprocessorEnvironment e, Map<byte[], ? extends Collection<?>> families) {
        HRegionInfo hri = e.getRegion().getRegionInfo();
        TableName tableName = hri.getTable();
        AuthResult result = null;


        return result;

    }

    AuthResult permissionGranted(OpType opType, User user, RegionCoprocessorEnvironment e, Map<byte[], ? extends Collection<?>> families, Action... actions) {
        AuthResult result = null;

        return result;
    }


    private User getActiveUser() throws IOException {
        User user = RpcServer.getRequestUser();
        if (user == null) {
            user = this.userProvider.getCurrent();
        }

        return user;
    }

    private void requirePermission(String request, TableName tableName, byte[] family, byte[] qualifier, Action... permissions) throws IOException {
        User user = this.getActiveUser();
        AuthResult result = null;

    }

    private void requireTablePermission(String request, TableName tableName, byte[] family, byte[] qualifier, Action... permissions) throws IOException {
        User user = this.getActiveUser();
        AuthResult result = null;

    }

    private void requireAccess(String request, TableName tableName, Action... permissions) throws IOException {
        User user = this.getActiveUser();
        AuthResult result = null;

    }

    private void requirePermission(String request, Action perm) throws IOException {
        this.requireGlobalPermission(request, perm, (TableName) null, (Map) null);
    }

    private void requireGlobalPermission(String request, Action perm, TableName tableName, Map<byte[], ? extends Collection<byte[]>> familyMap) throws IOException {
        User user = this.getActiveUser();
        AuthResult result = null;

    }

    private void requireGlobalPermission(String request, Action perm, String namespace) throws IOException {
        User user = this.getActiveUser();
        AuthResult authResult = null;

    }

    public void requireNamespacePermission(String request, String namespace, Action... permissions) throws IOException {
        User user = this.getActiveUser();
        AuthResult result = null;
    }

    public void requireNamespacePermission(String request, String namespace, TableName tableName, Map<byte[], ? extends Collection<byte[]>> familyMap, Action... permissions) throws IOException {
        User user = this.getActiveUser();
        AuthResult result = null;
    }

    private boolean hasFamilyQualifierPermission(User user, Action perm, RegionCoprocessorEnvironment env, Map<byte[], ? extends Collection<byte[]>> familyMap) throws IOException {
        HRegionInfo hri = env.getRegion().getRegionInfo();
        TableName tableName = hri.getTable();

        return true;
    }


    public void start(CoprocessorEnvironment env) throws IOException {
        CompoundConfiguration conf = new CompoundConfiguration();
        conf.add(env.getConfiguration());
        this.authorizationEnabled = isAuthorizationSupported(conf);
        if (!this.authorizationEnabled) {
            LOG.warn("The AccessController has been loaded with authorization checks disabled.");
        }

        this.shouldCheckExecPermission = conf.getBoolean("hbase.security.exec.permission.checks", false);
        this.cellFeaturesEnabled = HFile.getFormatVersion(conf) >= 3;
        if (!this.cellFeaturesEnabled) {
            LOG.info("A minimum HFile version of 3 is required to persist cell ACLs. Consider setting hfile.format.version accordingly.");
        }

        ZooKeeperWatcher zk = null;
        if (env instanceof MasterCoprocessorEnvironment) {
            MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) env;
            zk = mEnv.getMasterServices().getZooKeeper();
        } else if (env instanceof RegionServerCoprocessorEnvironment) {
            RegionServerCoprocessorEnvironment rsEnv = (RegionServerCoprocessorEnvironment) env;
            zk = rsEnv.getRegionServerServices().getZooKeeper();
        } else if (env instanceof RegionCoprocessorEnvironment) {
            this.regionEnv = (RegionCoprocessorEnvironment) env;
            conf.addStringMap(this.regionEnv.getRegion().getTableDesc().getConfiguration());
            zk = this.regionEnv.getRegionServerServices().getZooKeeper();
            this.compatibleEarlyTermination = conf.getBoolean("hbase.security.access.early_out", true);
        }

        this.userProvider = UserProvider.instantiate(env.getConfiguration());

    }

    public void stop(CoprocessorEnvironment env) {
    }

    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {

        this.requireNamespacePermission("createTable", desc.getTableName().getNamespaceAsString(), desc.getTableName(), null, Action.CREATE);
    }

    @Override
    public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> observerContext, PrepareBulkLoadRequest prepareBulkLoadRequest) throws IOException {

    }

    @Override
    public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> observerContext, CleanupBulkLoadRequest cleanupBulkLoadRequest) throws IOException {

    }

    @Override
    public Service getService() {
        return AccessControlService.newReflectiveService(this);
    }

    @Override
    public Message preEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Service service, String s, Message message) throws IOException {
        return null;
    }

    @Override
    public void postEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Service service, String s, Message message, Message.Builder builder) throws IOException {

    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1) throws IOException {

    }

    @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1, Region region2) throws IOException {

    }

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1, List<Mutation> list) throws IOException {

    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1, Region region2) throws IOException {

    }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1) throws IOException {

    }

    @Override
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1) throws IOException {

    }

    @Override
    public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, ReplicationEndpoint replicationEndpoint) {
        return null;
    }

    @Override
    public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, List<WALEntry> list, CellScanner cellScanner) throws IOException {

    }

    @Override
    public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, List<WALEntry> list, CellScanner cellScanner) throws IOException {

    }

    @Override
    public void grant(RpcController rpcController, GrantRequest grantRequest, RpcCallback<GrantResponse> rpcCallback) {
        log.info("========> grant()");
    }

    @Override
    public void revoke(RpcController rpcController, RevokeRequest revokeRequest, RpcCallback<RevokeResponse> rpcCallback) {

    }

    @Override
    public void getUserPermissions(RpcController rpcController, GetUserPermissionsRequest getUserPermissionsRequest, RpcCallback<GetUserPermissionsResponse> rpcCallback) {

    }

    @Override
    public void checkPermissions(RpcController rpcController, CheckPermissionsRequest checkPermissionsRequest, RpcCallback<CheckPermissionsResponse> rpcCallback) {

    }


    private static enum OpType {
        GET_CLOSEST_ROW_BEFORE("getClosestRowBefore"),
        GET("get"),
        EXISTS("exists"),
        SCAN("scan"),
        PUT("put"),
        DELETE("delete"),
        CHECK_AND_PUT("checkAndPut"),
        CHECK_AND_DELETE("checkAndDelete"),
        INCREMENT_COLUMN_VALUE("incrementColumnValue"),
        APPEND("append"),
        INCREMENT("increment");

        private String type;

        OpType(String type) {
            this.type = type;
        }

        public String toString() {
            return this.type;
        }
    }
}
