package cn.paic.bdsp.auth.hbase;


import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.*;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;

import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.MapMaker;


@Slf4j
public class BDSPCoprocessor implements MasterCoprocessor, RegionCoprocessor,
        RegionServerCoprocessor, AccessControlService.Interface,
        MasterObserver, RegionObserver, RegionServerObserver, EndpointObserver, BulkLoadObserver {


    private static final String TAG_CHECK_PASSED = "tag_check_passed";
    private static final byte[] TRUE = Bytes.toBytes(true);


    /**
     * defined only for Endpoint implementation, so it can have way to
     * access region services
     */
    private RegionCoprocessorEnvironment regionEnv;


    /**
     * if we are active, usually false, only true if "hbase.security.authorization"
     * has been set to true in site configuration
     */
    private boolean authorizationEnabled;

    /**
     * if we are able to support cell ACLs
     */
    private boolean cellFeaturesEnabled;

    /**
     * if we should check EXEC permissions
     */
    private boolean shouldCheckExecPermission;

    /**
     * if we should terminate access checks early as soon as table or CF grants
     * allow access; pre-0.98 compatible behavior
     */
    private boolean compatibleEarlyTermination;

    /**
     * if we have been successfully initialized
     */
    private volatile boolean initialized = false;

    /**
     * if the ACL table is available, only relevant in the master
     */
    private volatile boolean aclTabAvailable = false;

    public static boolean isCellAuthorizationSupported(Configuration conf) {
        return AccessChecker.isAuthorizationSupported(conf) &&
                (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS);
    }

    public Region getRegion() {
        return regionEnv != null ? regionEnv.getRegion() : null;
    }


    private AuthResult permissionGranted(String request, User user, Action permRequest,
                                         RegionCoprocessorEnvironment e,
                                         Map<byte[], ? extends Collection<?>> families) {
        RegionInfo hri = e.getRegion().getRegionInfo();
        TableName tableName = hri.getTable();

        // 1. All users need read access to hbase:meta table.
        // this is a very common operation, so deal with it quickly.
        if (hri.isMetaRegion()) {
            if (permRequest == Action.READ) {
                return AuthResult.allow(request, "All users allowed", user,
                        permRequest, tableName, families);
            }
        }

        if (user == null) {
            return AuthResult.deny(request, "No user associated with request!", null,
                    permRequest, tableName, families);
        }

        boolean flag = true;
        if (flag) {
            // no qualifiers and family-level check already failed
            return AuthResult.deny(request, "Failed family check", user, permRequest,
                    tableName, null, null);
        }
        // all family checks passed
        return AuthResult.allow(request, "All family checks passed", user, permRequest,
                tableName, families);
    }

    /**
     * Check the current user for authorization to perform a specific action
     * against the given set of row data.
     *
     * @param opType   the operation type
     * @param user     the user
     * @param e        the coprocessor environment
     * @param families the map of column families to qualifiers present in
     *                 the request
     * @param actions  the desired actions
     * @return an authorization result
     */
    private AuthResult permissionGranted(OpType opType, User user, RegionCoprocessorEnvironment e,
                                         Map<byte[], ? extends Collection<?>> families, Action... actions) {
        AuthResult result = null;
        for (Action action : actions) {
            result = permissionGranted(opType.toString(), user, action, e, families);
            if (!result.isAllowed()) {
                return result;
            }
        }
        return result;
    }


    /**
     * Returns <code>true</code> if the current user is allowed the given action
     * over at least one of the column qualifiers in the given column families.
     */
    private boolean hasFamilyQualifierPermission(User user,
                                                 Action perm,
                                                 RegionCoprocessorEnvironment env,
                                                 Map<byte[], ? extends Collection<byte[]>> familyMap) {

        RegionInfo hri = env.getRegion().getRegionInfo();
        TableName tableName = hri.getTable();

        if (user == null) {
            return false;
        }

        if (familyMap != null && familyMap.size() > 0) {
            log.debug("------------");
        } else if (log.isDebugEnabled()) {
            log.debug("Empty family map passed for permission check");
        }

        return false;
    }

    private enum OpType {
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

        private OpType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }

    }


    private static void addCellPermissions(final byte[] perms, Map<byte[], List<Cell>> familyMap) {
        // Iterate over the entries in the familyMap, replacing the cells therein
        // with new cells including the ACL data
        for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
            List<Cell> newCells = Lists.newArrayList();
            for (Cell cell : e.getValue()) {
                // Prepend the supplied perms in a new ACL tag to an update list of tags for the cell
                List<Tag> tags = new ArrayList<>();
                tags.add(new ArrayBackedTag(AccessControlLists.ACL_TAG_TYPE, perms));
                Iterator<Tag> tagIterator = PrivateCellUtil.tagsIterator(cell);
                while (tagIterator.hasNext()) {
                    tags.add(tagIterator.next());
                }
                newCells.add(PrivateCellUtil.createCell(cell, tags));
            }
            // This is supposed to be safe, won't CME
            e.setValue(newCells);
        }
    }

    // Checks whether incoming cells contain any tag with type as ACL_TAG_TYPE. This tag
    // type is reserved and should not be explicitly set by user.
    private void checkForReservedTagPresence(User user, Mutation m) throws IOException {
        // No need to check if we're not going to throw
        if (!authorizationEnabled) {
            m.setAttribute(TAG_CHECK_PASSED, TRUE);
            return;
        }
        // Superusers are allowed to store cells unconditionally.
        if (Superusers.isSuperUser(user)) {
            m.setAttribute(TAG_CHECK_PASSED, TRUE);
            return;
        }
        // We already checked (prePut vs preBatchMutation)
        if (m.getAttribute(TAG_CHECK_PASSED) != null) {
            return;
        }
        for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance(); ) {
            Iterator<Tag> tagsItr = PrivateCellUtil.tagsIterator(cellScanner.current());
            while (tagsItr.hasNext()) {
                if (tagsItr.next().getType() == AccessControlLists.ACL_TAG_TYPE) {
                    throw new AccessDeniedException("Mutation contains cell with reserved type tag");
                }
            }
        }
        m.setAttribute(TAG_CHECK_PASSED, TRUE);
    }

    /* ---- MasterObserver implementation ---- */
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        CompoundConfiguration conf = new CompoundConfiguration();
        conf.add(env.getConfiguration());

        authorizationEnabled = AccessChecker.isAuthorizationSupported(conf);
        if (!authorizationEnabled) {
            log.warn("AccessController has been loaded with authorization checks DISABLED!");
        }

        shouldCheckExecPermission = conf.getBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY,
                AccessControlConstants.DEFAULT_EXEC_PERMISSION_CHECKS);

        cellFeaturesEnabled = (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS);
        if (!cellFeaturesEnabled) {
            log.info("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
                    + " is required to persist cell ACLs. Consider setting " + HFile.FORMAT_VERSION_KEY
                    + " accordingly.");
        }

        ZKWatcher zk = null;
        if (env instanceof MasterCoprocessorEnvironment) {
            // if running on HMaster
            MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) env;
            if (mEnv instanceof HasMasterServices) {
                zk = ((HasMasterServices) mEnv).getMasterServices().getZooKeeper();
            }
        } else if (env instanceof RegionServerCoprocessorEnvironment) {
            RegionServerCoprocessorEnvironment rsEnv = (RegionServerCoprocessorEnvironment) env;
            if (rsEnv instanceof HasRegionServerServices) {
                zk = ((HasRegionServerServices) rsEnv).getRegionServerServices().getZooKeeper();
            }
        } else if (env instanceof RegionCoprocessorEnvironment) {
            // if running at region
            regionEnv = (RegionCoprocessorEnvironment) env;
            conf.addBytesMap(regionEnv.getRegion().getTableDescriptor().getValues());
            compatibleEarlyTermination = conf.getBoolean(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT,
                    AccessControlConstants.DEFAULT_ATTRIBUTE_EARLY_OUT);
            if (regionEnv instanceof HasRegionServerServices) {
                zk = ((HasRegionServerServices) regionEnv).getRegionServerServices().getZooKeeper();
            }
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        log.info("=========>stop");
    }

    /*********************************** Observer/Service Getters ***********************************/
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<EndpointObserver> getEndpointObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<BulkLoadObserver> getBulkLoadObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<RegionServerObserver> getRegionServerObserver() {
        return Optional.of(this);
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(
                AccessControlProtos.AccessControlService.newReflectiveService(this));
    }

    /*********************************** Observer implementations ***********************************/

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
                               TableDescriptor desc, RegionInfo[] regions) throws IOException {
        Set<byte[]> families = desc.getColumnFamilyNames();
        Map<byte[], Set<byte[]>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for (byte[] family : families) {
            familyMap.put(family, null);
        }
        log.info("====>preCreateTable");
    }

    @Override
    public void postCompletedCreateTableAction(
            final ObserverContext<MasterCoprocessorEnvironment> c,
            final TableDescriptor desc,
            final RegionInfo[] regions) throws IOException {
    }

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
            throws IOException {
        log.info("=======>preDeleteTable");
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
                                final TableName tableName) throws IOException {
        log.info("=======>postDeleteTable");
    }

    @Override
    public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> c,
                                 final TableName tableName) throws IOException {
        log.info("=======>preTruncateTable");
    }

    @Override
    public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  final TableName tableName) throws IOException {
        final Configuration conf = ctx.getEnvironment().getConfiguration();

        log.info("=======>postTruncateTable");
    }


    @Override
    public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
            throws IOException {
        log.info("=======>preEnableTable");
    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
            throws IOException {
        log.info("=======>preDisableTable");
    }

    @Override
    public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  final long procId) throws IOException {
        log.info("=======>preAbortProcedure");
    }

    @Override
    public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
        // There is nothing to do at this time after the procedure abort request was sent.
    }

    @Override
    public void preGetProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    @Override
    public void preGetLocks(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    @Override
    public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo region,
                        ServerName srcServer, ServerName destServer) throws IOException {
    }

    @Override
    public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo regionInfo)
            throws IOException {
    }

    @Override
    public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo regionInfo,
                            boolean force) throws IOException {
    }

    @Override
    public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c,
                                 RegionInfo regionInfo) throws IOException {
    }

    @Override
    public void preSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                          final boolean newValue, final MasterSwitchType switchType) throws IOException {
    }

    @Override
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c)
            throws IOException {
    }

    @Override
    public void preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c,
                                 boolean newValue) throws IOException {
    }

    @Override
    public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c)
            throws IOException {
    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c)
            throws IOException {
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    @Override
    public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                            final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
            throws IOException {
    }

    @Override
    public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                final SnapshotDescription snapshot) throws IOException {

    }

    @Override
    public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
            throws IOException {

    }

    @Override
    public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
            throws IOException {

    }

    @Override
    public void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  final SnapshotDescription snapshot) throws IOException {

    }

    @Override
    public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   NamespaceDescriptor ns) throws IOException {

    }

    @Override
    public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
            throws IOException {

    }

    @Override
    public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    final String namespace) throws IOException {
        final Configuration conf = ctx.getEnvironment().getConfiguration();
        log.info(namespace + " entry deleted in " + AccessControlLists.ACL_TABLE_NAME + " table.");
    }


    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
            throws IOException {
    }

    @Override
    public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                             List<NamespaceDescriptor> descriptors) throws IOException {
        // Retains only those which passes authorization checks, as the checks weren't done as part
        // of preGetTableDescriptors.

    }

    @Override
    public void preTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                              final TableName tableName) throws IOException {
        // Move this ACL check to MasterFlushTableProcedureManager#checkPermissions as part of AC
        // deprecation.
    }

    @Override
    public void preSplitRegion(
            final ObserverContext<MasterCoprocessorEnvironment> ctx,
            final TableName tableName,
            final byte[] splitRow) throws IOException {
    }

    @Override
    public void preClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    @Override
    public void preDecommissionRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                             List<ServerName> servers, boolean offload) throws IOException {
    }

    @Override
    public void preListDecommissionedRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    @Override
    public void preRecommissionRegionServer(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                            ServerName server, List<byte[]> encodedRegionNames) throws IOException {
    }

    /* ---- RegionObserver implementation ---- */

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c)
            throws IOException {

    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {

    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c,
                         FlushLifeCycleTracker tracker) throws IOException {
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
                                      CompactionRequest request) throws IOException {
        return scanner;
    }

    private void internalPreRead(final ObserverContext<RegionCoprocessorEnvironment> c,
                                 final Query query, OpType opType) throws IOException {
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c,
                         final Get get, final List<Cell> result) throws IOException {
    }

    @Override
    public boolean preExists(final ObserverContext<RegionCoprocessorEnvironment> c,
                             final Get get, final boolean exists) throws IOException {

        return true;
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
                       final Put put, final WALEdit edit, final Durability durability)
            throws IOException {

        // Require WRITE permission to the table, CF, or top visible value, if any.
        // NOTE: We don't need to check the permissions for any earlier Puts
        // because we treat the ACLs in each Put as timestamped like any other
        // HBase value. A new ACL in a new Put applies to that Put. It doesn't
        // change the ACL of any previous Put. This allows simple evolution of
        // security policy over time without requiring expensive updates.

    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
                        final Put put, final WALEdit edit, final Durability durability) {
        log.info("=======>postPut");
    }

    @Override
    public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
                          final Delete delete, final WALEdit edit, final Durability durability)
            throws IOException {
        // An ACL on a delete is useless, we shouldn't allow it
        log.info("=======>preDelete");

    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                               MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        log.info("=======>preBatchMutate");
    }

    @Override
    public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
                           final Delete delete, final WALEdit edit, final Durability durability)
            throws IOException {
        log.info("=======>postDelete");
    }

    @Override
    public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> c,
                                  final byte[] row, final byte[] family, final byte[] qualifier,
                                  final CompareOperator op,
                                  final ByteArrayComparable comparator, final Put put,
                                  final boolean result) throws IOException {
        log.info("=======>preCheckAndPut");
        return result;
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> c,
                                              final byte[] row, final byte[] family, final byte[] qualifier,
                                              final CompareOperator opp, final ByteArrayComparable comparator, final Put put,
                                              final boolean result) throws IOException {
        log.info("=======>preCheckAndPutAfterRowLock");
        return result;
    }

    @Override
    public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
                                     final byte[] row, final byte[] family, final byte[] qualifier,
                                     final CompareOperator op,
                                     final ByteArrayComparable comparator, final Delete delete,
                                     final boolean result) throws IOException {

        log.info("=======>preCheckAndDelete");
        return result;
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(
            final ObserverContext<RegionCoprocessorEnvironment> c, final byte[] row,
            final byte[] family, final byte[] qualifier, final CompareOperator op,
            final ByteArrayComparable comparator, final Delete delete, final boolean result) {

        log.info("=======>preCheckAndDelete");
        return result;
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
            throws IOException {
        log.info("=======>preAppend");
        return null;
    }

    @Override
    public Result preAppendAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> c,
                                        final Append append) throws IOException {

        log.info("=======>preAppendAfterRowLock");
        return null;
    }

    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
                               final Increment increment)
            throws IOException {
        log.info("=======>preIncrement");
        return null;
    }

    @Override
    public Result preIncrementAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> c,
                                           final Increment increment) throws IOException {

        log.info("=======>preIncrementAfterRowLock");
        return null;
    }

    @Override
    public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx,
                                      MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
        // If the HFile version is insufficient to persist tags, we won't have any
        // work to do here
        log.info("=======>postMutationBeforeWAL");
        return null;
    }

    @Override
    public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan)
            throws IOException {
        log.info("=======>preScannerOpen");
    }

    @Override
    public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
                                         final Scan scan, final RegionScanner s) throws IOException {
        log.info("=======>postScannerOpen");
        return s;
    }

    @Override
    public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
                                  final InternalScanner s, final List<Result> result,
                                  final int limit, final boolean hasNext) throws IOException {
        log.info("=======>preScannerNext");
        return hasNext;
    }

    @Override
    public void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
                                final InternalScanner s) throws IOException {
        log.info("=======>preScannerClose");
    }

    @Override
    public void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
                                 final InternalScanner s) throws IOException {
        // clean up any associated owner mapping
        log.info("=======>postScannerClose");
    }

    @Override
    public boolean postScannerFilterRow(final ObserverContext<RegionCoprocessorEnvironment> e,
                                        final InternalScanner s, final Cell curRowCell, final boolean hasMore) throws IOException {
        // 'default' in RegionObserver might do unnecessary copy for Off heap backed Cells.
        log.info("=======>postScannerFilterRow");
        return hasMore;
    }

    /**
     * Verify, when servicing an RPC, that the caller is the scanner owner.
     * If so, we assume that access control is correctly enforced based on
     * the checks performed in preScannerOpen()
     */
    private void requireScannerOwner(InternalScanner s) throws AccessDeniedException {
        log.info("=======>requireScannerOwner");
    }

    /**
     * Verifies user has CREATE privileges on
     * the Column Families involved in the bulkLoadHFile
     * request. Specific Column Write privileges are presently
     * ignored.
     */
    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
                                 List<Pair<byte[], String>> familyPaths) throws IOException {
        log.info("=======>preBulkLoadHFile");
    }

    /**
     * Authorization check for
     * SecureBulkLoadProtocol.prepareBulkLoad()
     *
     * @param ctx the context
     * @throws IOException
     */
    @Override
    public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        log.info("=======>prePrepareBulkLoad");
    }

    /**
     * Authorization security check for
     * SecureBulkLoadProtocol.cleanupBulkLoad()
     *
     * @param ctx the context
     * @throws IOException
     */
    @Override
    public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        log.info("=======>preCleanupBulkLoad");
    }

    @Override
    public void grant(RpcController rpcController, AccessControlProtos.GrantRequest grantRequest, RpcCallback<AccessControlProtos.GrantResponse> rpcCallback) {
        log.info("=====>grant");
    }

    @Override
    public void revoke(RpcController rpcController, AccessControlProtos.RevokeRequest revokeRequest, RpcCallback<AccessControlProtos.RevokeResponse> rpcCallback) {
        log.info("=====>revoke");
    }

    @Override
    public void getUserPermissions(RpcController rpcController, AccessControlProtos.GetUserPermissionsRequest getUserPermissionsRequest, RpcCallback<AccessControlProtos.GetUserPermissionsResponse> rpcCallback) {
        log.info("=====>getUserPermissions");
    }

    @Override
    public void checkPermissions(RpcController rpcController, AccessControlProtos.CheckPermissionsRequest checkPermissionsRequest, RpcCallback<AccessControlProtos.CheckPermissionsResponse> rpcCallback) {
        log.info("=====>checkPermissions");
    }
}
