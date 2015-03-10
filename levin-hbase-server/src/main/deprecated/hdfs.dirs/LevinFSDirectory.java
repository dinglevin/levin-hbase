package org.levin.hadoop.hdfs.dirs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.AclStorage;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeMap;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.XAttrStorage;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class LevinFSDirectory {
    private static final Logger logger = LoggerFactory.getLogger(LevinFSDirectory.class);
    
    private final LevinFSNamesystem namesystem;
    private final INodeDirectory rootDir;
    private final INodeMap inodeMap;
    
    private final int maxComponentLength;
    private final int maxDirItems;
    
    private volatile boolean skipQuotaCheck = false;
    
    public LevinFSDirectory(LevinFSNamesystem namesystem, Configuration conf) {
        this.namesystem = namesystem;
        this.rootDir = LevinFSDirectoryUtil.createRoot(namesystem);
        this.inodeMap = INodeMap.newInstance(rootDir);
        
        this.maxComponentLength = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 
                DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
        this.maxDirItems = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 
                DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);
    }
    
    public INodeDirectory getRootDir() {
        return rootDir;
    }
    
    public INodeMap getINodeMap() {
        return inodeMap;
    }
    
    public long delete(String src, BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes, long mtime) 
            throws SnapshotAccessControlException, UnresolvedLinkException, 
            SnapshotException, QuotaExceededException {
        long filesRemoved;
        INodesInPath iip = getINodesInPath4Write(normalizePath(src), false);
        if (!deleteAllowed(iip, src)) {
            filesRemoved = -1;
        } else {
            List<INodeDirectorySnapshottable> snapshottableDirs = Lists.newArrayList();
            checkSnapshot(iip.getLastINode(), snapshottableDirs);
            filesRemoved = unprotectedDelete(iip, collectedBlocks, removedINodes, mtime);
            namesystem.removeSnapshottableDirs(snapshottableDirs);
        }
        
        return filesRemoved;
    }
    
    public void unprotectedDelete(String src, long mtime) 
            throws SnapshotAccessControlException, UnresolvedLinkException, 
            SnapshotException, QuotaExceededException {
        BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
        List<INode> removedINodes = new ChunkedArrayList<>();
        
        INodesInPath iip = getINodesInPath4Write(normalizePath(src), false);
        long filesRemoved = -1;
        if (deleteAllowed(iip, src)) {
            List<INodeDirectorySnapshottable> snapshottableDirs = Lists.newArrayList();
            checkSnapshot(iip.getLastINode(), snapshottableDirs);
            filesRemoved = unprotectedDelete(iip, collectedBlocks, removedINodes, mtime);
            namesystem.removeSnapshottableDirs(snapshottableDirs);
        }
        
        if (filesRemoved >= 0) {
            namesystem.removePathAndBlocks(src, collectedBlocks, removedINodes, false);
        }
    }
    
    private long unprotectedDelete(INodesInPath iip, BlocksMapUpdateInfo collectedBlocks, 
            List<INode> removedINodes, long mtime) throws QuotaExceededException {
        INode targetNode = iip.getLastINode();
        if (targetNode == null) {
            return -1;
        }
        
        int latestSnapshot = iip.getLatestSnapshotId();
        targetNode = targetNode.recordModification(latestSnapshot);
        iip.setLastINode(targetNode);
        
        long removed = removeLastINode(iip);
        if (removed == -1) {
            return -1;
        }
        
        INodeDirectory parent = targetNode.getParent();
        parent.updateModificationTime(mtime, latestSnapshot);
        if (removed == 0) {
            return 0;
        }
        
        if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
            targetNode.destroyAndCollectBlocks(collectedBlocks, removedINodes);
        } else {
            Quota.Counts counts = targetNode.cleanSubtree(Snapshot.CURRENT_STATE_ID, latestSnapshot, 
                    collectedBlocks, removedINodes, true);
            parent.addSpaceConsumed(-counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE), true);
            removed = counts.get(Quota.NAMESPACE);
        }
        
        return removed;
    }
    
    private static boolean deleteAllowed(INodesInPath iip, String src) {
        INode[] inodes = iip.getINodes();
        if (inodes == null || inodes.length == 0 || inodes[inodes.length - 1] == null) {
            return false;
        } else if (inodes.length == 1) {
            return false;
        }
        return true;
    }
    
    private static String normalizePath(String src) {
        if (src.length() > 1 && src.endsWith("/")) {
            src = src.substring(0, src.length() - 1);
        }
        return src;
    }
    
    public void unprotectedConcat(String target, String[] srcs, long timestamp) 
            throws SnapshotAccessControlException, UnresolvedLinkException, 
            SnapshotException, QuotaExceededException {
        INodesInPath trgIIP = getINodesInPath4Write(target, true);
        INodeFile trgINode = trgIIP.getLastINode().asFile();
        INodeDirectory trgParent = trgIIP.getINode(-2).asDirectory();
        int trgSnapshot = trgIIP.getLatestSnapshotId();
        
        INodeFile[] srcINodes = new INodeFile[srcs.length];
        for (int i = 0; i < srcs.length; i++) {
            INodesInPath iip = getINodesInPath4Write(srcs[i], true);
            int srcSnapshot = iip.getLatestSnapshotId();
            INode inode = iip.getLastINode();
            
            if (inode.isInLatestSnapshot(srcSnapshot)) {
                throw new SnapshotException("Concat: source file " + srcs[i] + 
                        " is in snapshot " + srcSnapshot);
            }
            
            if (inode.isReference() && ((INodeReference.WithCount) inode.asReference().getReferredINode()).getReferenceCount() > 1) {
                throw new SnapshotException("Concat: source file " + srcs[i] + 
                        " is referred by some other reference in some snapshot");
            }
            
            srcINodes[i] = inode.asFile();
        }
        
        trgINode.concatBlocks(srcINodes);
        
        int count = 0;
        for (INodeFile nodeToRemove : srcINodes) {
            if (nodeToRemove == null) continue;
            
            nodeToRemove.setBlocks(null);
            trgParent.removeChild(nodeToRemove, trgSnapshot);
            inodeMap.remove(nodeToRemove);
            count++;
        }
        
        removeFromInodeMap(Arrays.asList(srcINodes));
        
        trgINode.setModificationTime(timestamp, trgSnapshot);
        trgParent.updateModificationTime(timestamp, trgSnapshot);
        
        unprotectedUpdateCount(trgIIP, trgIIP.getINodes().length - 1, -count, 0);
    }
    
    private void removeFromInodeMap(List<? extends INode> inodes) {
        if (inodes != null) {
            for (INode inode : inodes) {
                if (inode != null && inode instanceof INodeWithAdditionalFields) {
                    inodeMap.remove(inode);
                }
            }
        }
    }
    
    public void unprotectedSetOwner(String src, String username, String groupname) 
            throws SnapshotAccessControlException, UnresolvedLinkException, 
            FileNotFoundException, QuotaExceededException {
        INodesInPath iip = getINodesInPath4Write(src, true);
        INode inode = iip.getLastINode();
        if (inode == null) {
            throw new FileNotFoundException("File not exists: " + src);
        }
        
        int snapshotId = iip.getLatestSnapshotId();
        if (username != null) {
            inode.setUser(username, snapshotId);
        }
        if (groupname != null) {
            inode.setGroup(groupname, snapshotId);
        }
    }
    
    public void unprotectedSetPermission(String src, FsPermission permission) 
            throws UnresolvedLinkException, FileNotFoundException, 
            QuotaExceededException, SnapshotAccessControlException {
        INodesInPath iip = getINodesInPath4Write(src, true);
        INode inode = iip.getLastINode();
        if (inode == null) {
            throw new FileNotFoundException("File not exists: " + src);
        }
        
        int snapshotId = iip.getLatestSnapshotId();
        inode.setPermission(permission, snapshotId);
    }
    
    public long getPreferredBlockSize(String path) 
            throws FileNotFoundException, UnresolvedLinkException {
        return INodeFile.valueOf(getNode(path, false), path).getPreferredBlockSize();
    }
    
    private INode getNode(String path, boolean resolveLink) 
            throws UnresolvedLinkException {
        return getLastINodeInPath(path, resolveLink).getINode(0);
    }
    
    private INodesInPath getLastINodeInPath(String path, boolean resolveLink) 
            throws UnresolvedLinkException {
        return INodesInPath.resolve(rootDir, INode.getPathComponents(path), 1, resolveLink);
    }
    
    private INodesInPath getINodesInPath(String path, boolean resolveLink) 
            throws UnresolvedLinkException {
        byte[][] components = INode.getPathComponents(path);
        return INodesInPath.resolve(rootDir, components, components.length, resolveLink);
    }
    
    private INode getINode4Write(String src, boolean resolveLink) 
            throws SnapshotAccessControlException, UnresolvedLinkException {
        return getINodesInPath4Write(src, resolveLink).getLastINode();
    }
    
    public Block[] unprotectedSetReplication(String src, short replication, short[] blockRepls) 
            throws SnapshotAccessControlException, UnresolvedLinkException, QuotaExceededException {
        INodesInPath iip = getINodesInPath4Write(src, true);
        INode inode = iip.getLastINode();
        if (inode == null || !inode.isFile()) {
            return null;
        }
        
        INodeFile file = inode.asFile();
        short oldBR = file.getBlockReplication();
        if (replication > oldBR) {
            long dsDelta = (replication - oldBR) * (file.diskspaceConsumed() / oldBR);
            updateCount(iip, 0, dsDelta, true);
        }
        
        file = file.setFileReplication(replication, iip.getLatestSnapshotId(), inodeMap);
        short newBR = file.getBlockReplication();
        if (newBR < oldBR) {
            long dsDelta = (newBR - oldBR) * (file.diskspaceConsumed() / newBR);
            updateCount(iip, 0, dsDelta, true);
        }
        
        if (blockRepls != null) {
            blockRepls[0] = oldBR;
            blockRepls[1] = newBR;
        }
        
        return file.getBlocks();
    }
    
    public void renameTo(String src, String dst, long mtime, Options.Rename... options) 
            throws IOException {
        if (unprotectedRenameTo(src, dst, mtime, options)) {
            namesystem.incrDeletedFileCount(1);
        }
    }
    
    private boolean unprotectedRenameTo(String src, String dst, long mtime, Options.Rename... options) 
            throws FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException,
            QuotaExceededException, UnresolvedLinkException, IOException {
        boolean override = options != null && Arrays.asList(options).contains(Options.Rename.OVERWRITE);
        
        INodesInPath srcIIP = getINodesInPath4Write(src, false);
        INode srcINode = srcIIP.getLastINode();
        validateRenameSource(src, srcIIP);
        
        if (dst.equals(src)) {
            throw new FileAlreadyExistsException("The source " + src + " and destination " + dst + " are the same");
        }
        validateRenameDestination(src, dst, srcINode);
        
        INodesInPath dstIIP = getINodesInPath4Write(dst, false);
        if (dstIIP.getINodes().length == 1) {
            throw new IOException("Rename destination cannot be root");
        }
        
        INode dstINode = dstIIP.getLastINode();
        List<INodeDirectorySnapshottable> snapshottableDirs = Lists.newArrayList();
        if (dstINode != null) {
            validateRenameOverride(src, dst, override, srcINode, dstINode);
            checkSnapshot(dstINode, snapshottableDirs);
        }
        
        INode dstParent = dstIIP.getINode(-2);
        if (dstParent == null) {
            throw new FileNotFoundException("Rename destination parent " + dst + " not found");
        }
        if (!dstParent.isDirectory()) {
            throw new ParentNotDirectoryException("Rename destination parent " + dst + " is a file");
        }
        
        verifyFsLimitsForRename(srcIIP, dstIIP);
        verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());
        
        RenameOperation tx = new RenameOperation(src, dst, srcIIP, dstIIP);
        
        boolean undoRemoveSrc = true;
        long removedSrc = removeLastINode(srcIIP);
        if (removedSrc == -1) {
            throw new IOException("Failed to rename " + src + " to " + dst + 
                    " because source cannot be removed");
        }
        
        boolean undoRemoveDst = false;
        INode removedDst = null;
        long removedNum = 0;
        try {
            if (dstINode != null) {
                if ((removedNum = removeLastINode(dstIIP)) != -1) {
                    removedDst = dstIIP.getLastINode();
                    undoRemoveDst = true;
                }
            }
            
            if (tx.addSourceToDestination()) {
                undoRemoveSrc = false;
                logger.info(src + " is renamed to " + dst);
                
                tx.updateMtimeAndLease(mtime);
                
                long filesDeleted = -1;
                if (removedDst != null) {
                    undoRemoveDst = false;
                    if (removedNum > 0) {
                        BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
                        List<INode> removedINodes = new ChunkedArrayList<>();
                        filesDeleted = removedDst.cleanSubtree(Snapshot.CURRENT_STATE_ID, 
                                dstIIP.getLatestSnapshotId(), collectedBlocks, removedINodes, 
                                true).get(Quota.NAMESPACE);
                        namesystem.removePathAndBlocks(src, collectedBlocks, removedINodes, false);
                    }
                }
                
                if (snapshottableDirs.size() > 0) {
                    namesystem.removeSnapshottableDirs(snapshottableDirs);
                }
                
                tx.updateQuotasInSourceTree();
                return filesDeleted >= 0;
            }
        } finally {
            if (undoRemoveSrc) {
                tx.restoreSource();
            }
            
            if (undoRemoveDst) {
                if (dstParent.isDirectory() && dstParent.asDirectory().isWithSnapshot()) {
                    dstParent.asDirectory().undoRename4DstParent(removedDst, dstIIP.getLatestSnapshotId());
                } else {
                    addLastINodeNoQuotaCheck(dstIIP, removedDst);
                }
                if (removedDst.isReference()) {
                    INodeReference removedDstRef = removedDst.asReference();
                    INodeReference.WithCount wc = (WithCount) removedDstRef.getReferredINode().asReference();
                    wc.addReference(removedDstRef);
                }
            }
        }
        
        throw new IOException("Rename from " + src + " to " + dst + " failed");
    }
    
    private class RenameOperation {
        private final INodesInPath srcIIP;
        private final INodesInPath dstIIP;
        private final String src;
        private final String dst;
        
        private INode srcChild;
        private final INodeReference.WithCount withCount;
        private final int srcRefDstSnapshot;
        private final INodeDirectory srcParent;
        private final byte[] srcChildName;
        private final boolean isSrcInSnapshot;
        private final boolean srcChildIsReference;
        private final Quota.Counts oldSrcCounts;
        
        private RenameOperation(String src, String dst, INodesInPath srcIIP, INodesInPath dstIIP) 
                throws QuotaExceededException {
            this.srcIIP = srcIIP;
            this.dstIIP = dstIIP;
            this.src = src;
            this.dst = dst;
            
            srcChild = srcIIP.getLastINode();
            srcChildName = srcChild.getLocalNameBytes();
            isSrcInSnapshot = srcChild.isInLatestSnapshot(srcIIP.getLatestSnapshotId());
            srcChildIsReference = srcChild.isReference();
            srcParent = srcIIP.getINode(-2).asDirectory();
            
            if (isSrcInSnapshot) {
                srcChild = srcChild.recordModification(srcIIP.getLatestSnapshotId());
            }
            
            srcRefDstSnapshot = srcChildIsReference ? srcChild.asReference().getDstSnapshotId() : Snapshot.CURRENT_STATE_ID;
            oldSrcCounts = Quota.Counts.newInstance();
            if (isSrcInSnapshot) {
                INodeReference.WithName withName = srcIIP.getINode(-2).asDirectory().replaceChild4ReferenceWithName(srcChild, srcIIP.getLatestSnapshotId());
                withCount = (INodeReference.WithCount) withName.getReferredINode();
                srcChild = withName;
                srcIIP.setLastINode(srcChild);
                withCount.getReferredINode().computeQuotaUsage(oldSrcCounts, true);
            } else if (srcChildIsReference) {
                withCount = (WithCount) srcChild.asReference().getReferredINode();
            } else {
                withCount = null;
            }
        }
        
        boolean addSourceToDestination() {
            INode dstParent = dstIIP.getINode(-2);
            srcChild = srcIIP.getLastINode();
            byte[] dstChildName = dstIIP.getLastLocalName();
            
            INode toDst;
            if (withCount == null) {
                srcChild.setLocalName(dstChildName);;
                toDst = srcChild;
            } else {
                withCount.getReferredINode().setLocalName(dstChildName);
                int dstSnapshotId = dstIIP.getLatestSnapshotId();
                toDst = new INodeReference.DstReference(dstParent.asDirectory(), withCount, dstSnapshotId);
            }
            
            return addLastINodeNoQuotaCheck(dstIIP, toDst);
        }
        
        void updateMtimeAndLease(long mtime) throws QuotaExceededException {
            srcParent.updateModificationTime(mtime, srcIIP.getLatestSnapshotId());
            INode dstParent = dstIIP.getINode(-2);
            dstParent.updateModificationTime(mtime, dstIIP.getLatestSnapshotId());
            namesystem.unprotectedChangeLease(src, dst);
        }
        
        void restoreSource() throws QuotaExceededException {
            INode oldSrcChild = srcChild;
            if (withCount == null) {
                srcChild.setLocalName(srcChildName);
            } else if (!srcChildIsReference) {
                srcChild = withCount.getReferredINode();
                srcChild.setLocalName(srcChildName);
            } else {
                withCount.removeReference(oldSrcChild.asReference());
                srcChild = new INodeReference.DstReference(srcParent, withCount, srcRefDstSnapshot);
                withCount.getReferredINode().setLocalName(srcChildName);
            }
            
            if (isSrcInSnapshot) {
                srcParent.undoRename4ScrParent(oldSrcChild.asReference(), srcChild);
            } else {
                addLastINodeNoQuotaCheck(srcIIP, srcChild);
            }
        }
        
        void updateQuotasInSourceTree() throws QuotaExceededException {
            if (isSrcInSnapshot) {
                Quota.Counts newSrcCounts = srcChild.computeQuotaUsage(Quota.Counts.newInstance(), false);
                newSrcCounts.subtract(oldSrcCounts);
                srcParent.addSpaceConsumed(newSrcCounts.get(Quota.NAMESPACE), 
                        newSrcCounts.get(Quota.DISKSPACE), false);
            }
        }
    }
    
    private boolean addLastINodeNoQuotaCheck(INodesInPath iip, INode inode) {
        try {
            return addLastINode(iip, inode, false);
        } catch (QuotaExceededException e) {
            logger.warn("addChildNoQuotaCheck - unexpected");
        }
        return false;
    }
    
    private long removeLastINode(INodesInPath iip) throws QuotaExceededException {
        int latestSnapshot = iip.getLatestSnapshotId();
        INode last = iip.getLastINode();
        INodeDirectory parent = iip.getINode(-2).asDirectory();
        if (!parent.removeChild(last, latestSnapshot)) {
            return -1;
        }
        
        if (!last.isInLatestSnapshot(latestSnapshot)) {
            Quota.Counts counts = last.computeQuotaUsage();
            updateCountNoQuotaCheck(iip, iip.getINodes().length - 1, 
                    -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));
            if (INodeReference.tryRemoveReference(last) > 0) {
                return 0;
            } else {
                return counts.get(Quota.NAMESPACE);
            }
        }
        return 1;
    }
    
    private void verifyFsLimitsForRename(INodesInPath srcIIP, INodesInPath dstIIP) 
            throws PathComponentTooLongException, MaxDirectoryItemsExceededException {
        byte[] dstChildName = dstIIP.getLastLocalName();
        INode[] dstINodes = dstIIP.getINodes();
        int pos = dstINodes.length - 1;
        verifyMaxComponentLength(dstChildName, dstINodes, pos);
        if (srcIIP.getINode(-2) != dstIIP.getINode(-2)) {
            verifyMaxDirItems(dstINodes, pos);
        }
    }
    
    private void verifyQuotaForRename(INode[] src, INode[] dst) throws QuotaExceededException {
        if (skipQuotaCheck) {
            return;
        }
        
        int i = 0;
        while (src[i] == dst[i]) { i++; }
        
        Quota.Counts delta = src[src.length - 1].computeQuotaUsage();
        int dstIndex = dst.length - 1;
        if (dst[dstIndex] != null) {
            delta.subtract(dst[dstIndex].computeQuotaUsage());
        }
        verifyQuota(dst, dstIndex, delta.get(Quota.NAMESPACE), delta.get(Quota.DISKSPACE), src[i - 1]);
    }
    
    private static void verifyQuota(INode[] inodes, int pos, long nsDelta, long dsDelta, INode commonAccessor) 
            throws QuotaExceededException {
        if (nsDelta <= 0 && dsDelta <= 0) {
            return;
        }
        
        for (int i = (pos > inodes.length ? inodes.length : pos) - 1; i >= 0; i--) {
            if (commonAccessor == inodes[i]) {
                return;
            }
            DirectoryWithQuotaFeature q = inodes[i].asDirectory().getDirectoryWithQuotaFeature();
            if (q != null) {
                try {
                    q.verifyQuota(nsDelta, dsDelta);
                } catch (QuotaExceededException e) {
                    e.setPathName(getFullPathName(inodes, i));
                    throw e;
                }
            }
        }
    }
    
    private static void validateRenameOverride(String src, String dst, boolean override, 
            INode srcINode, INode dstINode) throws IOException {
        if (srcINode.isDirectory() != dstINode.isDirectory()) {
            throw new IOException("Source " + src + " and destination " + dst + " must both dirs or files");
        }
        
        if (!override) {
            throw new FileAlreadyExistsException("Rename destination " + dst + " already exists");
        }
        
        if (dstINode.isDirectory()) {
            ReadOnlyList<INode> children = dstINode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
            if (!children.isEmpty()) {
                throw new IOException("Rename destination directory is not empty: " + dst);
            }
        }
    }
    
    private static void validateRenameSource(String src, INodesInPath iip) throws IOException {
        INode srcINode = iip.getLastINode();
        if (srcINode == null) {
            logger.error("rename source " + src + " is not found");
            throw new FileNotFoundException(src);
        }
        
        if (iip.getINodes().length == 1) {
            logger.error("rename source cannot be root");
            throw new IOException("rename source cannot be root");
        }
        
        checkSnapshot(srcINode, null);
    }
    
    private static void validateRenameDestination(String src, String dst, INode srcINode) 
            throws IOException {
        if (srcINode.isSymlink() && dst.equals(srcINode.asSymlink().getSymlinkString())) {
            throw new FileAlreadyExistsException("Cannot rename symlink " + src + " to its target " + dst);
        }
        
        if (dst.startsWith(src) && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
            throw new IOException("Rename destination " + dst + " is a directory or file under source " + src);
        }
    }
    
    private static void checkSnapshot(INode target, List<INodeDirectorySnapshottable> snapshottableDirs) 
            throws SnapshotException {
        if (target.isDirectory()) {
            INodeDirectory targetDir = target.asDirectory();
            if (targetDir.isSnapshottable()) {
                INodeDirectorySnapshottable ssTargetDir = (INodeDirectorySnapshottable) targetDir;
                if (ssTargetDir.getNumSnapshots() > 0) {
                    throw new SnapshotException("The directory " + ssTargetDir.getFullPathName() + 
                            " cannot be deleted since " + ssTargetDir.getFullPathName() + 
                            " is snapshottable, and already has snapshots");
                } else {
                    if (snapshottableDirs != null) {
                        snapshottableDirs.add(ssTargetDir);
                    }
                }
            }
            
            for (INode child : targetDir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
                checkSnapshot(child, snapshottableDirs);
            }
        }
    }
    
    public boolean removeBlock(String path, INodeFile inodeFile, Block block) throws IOException {
        Preconditions.checkArgument(inodeFile.isUnderConstruction());
        return unprotectedRemoveBlock(path, inodeFile, block);
    }
    
    private boolean unprotectedRemoveBlock(String path, INodeFile inodeFile, Block block) throws IOException {
        boolean removed = inodeFile.removeLastBlock(block);
        if (!removed) {
            return false;
        }
        
        namesystem.getBlockManager().removeBlockFromMap(block);
        logger.info("remove block: " + path + " with " + block);
        
        INodesInPath iip = getINodesInPath4Write(path, true);
        updateCount(iip, 0, -inodeFile.getBlockDiskspace(), true);
        return true;
    }
    
    public void completeFile(String path) throws SnapshotAccessControlException, UnresolvedLinkException {
        INodesInPath iip = getINodesInPath4Write(path, false);
        iip.getLastINode().asFile().toCompleteFile(System.currentTimeMillis());
    }
    
    public BlockInfo addBlock(String path, DatanodeStorageInfo[] targets) 
            throws SnapshotAccessControlException, 
            UnresolvedLinkException, IOException {
        return addBlock(path, getINodesInPath4Write(path, false), 
                namesystem.createNewBlock(), targets);
    }
    
    public BlockInfo addBlock(String path, INodesInPath iip, Block block, 
            DatanodeStorageInfo[] targets) throws IOException {
        INodeFile inodeFile = iip.getLastINode().asFile();
        Preconditions.checkState(inodeFile.isUnderConstruction());
        
        updateCount(iip, 0, inodeFile.getBlockDiskspace(), true);
        
        BlockInfoUnderConstruction blockInfo = new BlockInfoUnderConstruction(
                block, inodeFile.getFileReplication(), BlockUCState.UNDER_CONSTRUCTION, targets);
        namesystem.getBlockManager().addBlockConllection(blockInfo, inodeFile);
        inodeFile.addBlock(blockInfo);
        
        logger.info("add block: " + block + " to: " + path);
        
        return blockInfo;
    }
    
    public void updateSpaceConsumed(String path, long nsDelta, long dsDelta) 
            throws SnapshotAccessControlException, UnresolvedLinkException, 
            FileNotFoundException, QuotaExceededException {
        INodesInPath iip = getINodesInPath4Write(path, false);
        if (iip.getLastINode() == null) {
            throw new FileNotFoundException("Path not found: " + path);
        }
        updateCount(iip, nsDelta, dsDelta, true);
    }
    
    private INodesInPath getINodesInPath4Write(String src, boolean resolveLink) 
            throws UnresolvedLinkException, SnapshotAccessControlException {
        byte[][] components = INode.getPathComponents(src);
        INodesInPath iip = INodesInPath.resolve(rootDir, components, components.length, resolveLink);
        if (iip.isSnapshot()) {
            throw new SnapshotAccessControlException("Modification an read-only snapshot is disallowed");
        }
        return iip;
    }
    
    private void updateCount(INodesInPath iip, long nsDelta, long dsDelta, 
            boolean checkQuota) throws QuotaExceededException {
        updateCount(iip, iip.getINodes().length - 1, nsDelta, dsDelta, checkQuota);
    }
    
    public INodeFile addFile(String path, PermissionStatus permissions, short replication,
            long preferredBlockSize, String clientName, String clientMachine) 
                    throws FileAlreadyExistsException, QuotaExceededException, 
                    UnresolvedLinkException {
        long mtime = System.currentTimeMillis();
        INodeFile newFile = new INodeFile(namesystem.nextINodeId(), null, permissions,
                mtime, mtime, BlockInfo.EMPTY_ARRAY, replication, preferredBlockSize);
        newFile.toUnderConstruction(clientName, clientMachine);
        
        boolean added = addINode(path, newFile);
        if (!added) {
            logger.info("Failed to add file: " + path);
            return null;
        }
        
        logger.info("Added file: " + path);
        return newFile;
    }
    
    public INodeFile unprotectedAddFile(long inodeId, String path, PermissionStatus permissions,
            List<AclEntry> aclEntries, List<XAttr> xAttrs, short replication, long mtime,
            long atime, long preferredBlockSize, boolean underConstruction, String clientName, 
            String clientMachine) {
        INodeFile newFile;
        if (underConstruction) {
            newFile = new INodeFile(inodeId, null, permissions, mtime, mtime, 
                    BlockInfo.EMPTY_ARRAY, replication, preferredBlockSize);
            newFile.toUnderConstruction(clientName, clientMachine);
        } else {
            newFile = new INodeFile(inodeId, null, permissions, mtime, atime, 
                    BlockInfo.EMPTY_ARRAY, replication, preferredBlockSize);
        }
        
        try {
            if (addINode(path, newFile)) {
                if (aclEntries != null) {
                    AclStorage.updateINodeAcl(newFile, aclEntries, Snapshot.CURRENT_STATE_ID);
                }
                if (xAttrs != null) {
                    XAttrStorage.updateINodeXAttrs(newFile, xAttrs, Snapshot.CURRENT_STATE_ID);
                }
                return newFile;
            }
        } catch (IOException e) {
            logger.error("unprotectedAddFile failed to add path: " + path, e);
        }
        
        return null;
    }
    
    private boolean addINode(String src, INode child) 
            throws QuotaExceededException, UnresolvedLinkException {
        byte[][] components = INode.getPathComponents(src);
        child.setLocalName(components[components.length - 1]);
        return addLastINode(getExistingPathINodes(components), child, true);
    }
    
    private boolean addLastINode(INodesInPath iip, INode inode, boolean checkQuota) throws QuotaExceededException {
        int pos = iip.getINodes().length - 1;
        return addChild(iip, pos, inode, checkQuota);
    }
    
    public INode unprotectedMkdir(long inodeId, String src, PermissionStatus perms,
            List<AclEntry> aclEntries, long timestamp) 
                    throws UnresolvedLinkException, AclException, QuotaExceededException {
        byte[][] components = INode.getPathComponents(src);
        INodesInPath iip = getExistingPathINodes(components);
        INode[] inodes = iip.getINodes();
        final int pos = inodes.length - 1;
        unprotectedMkdir(inodeId, iip, pos, components[pos], 
                perms, aclEntries, timestamp);
        return inodes[pos];
    }
    
    private void unprotectedMkdir(long inodeId, INodesInPath inodesInPath, int pos,
            byte[] name, PermissionStatus perms, List<AclEntry> aclEntries, long timestamp) 
                    throws AclException, QuotaExceededException {
        INodeDirectory dir = new INodeDirectory(inodeId, name, perms, timestamp);
        if (addChild(inodesInPath, pos, dir, true)) {
            if (aclEntries != null) {
                AclStorage.updateINodeAcl(dir, aclEntries, Snapshot.CURRENT_STATE_ID);
            }
            inodesInPath.setINode(pos, dir);
        }
    }
    
    private boolean addChild(INodesInPath iip, int pos, INode child, boolean checkQuota) 
            throws QuotaExceededException {
        INode[] inodes = iip.getINodes();
        if (pos == 1 && inodes[0] == rootDir && isReservedName(child)) {
            throw new HadoopIllegalArgumentException(
                    "File name \"" + child.getLocalName() + "\" is reserved and cannot "
                        + "be created. If this is during upgrade change the name of the "
                        + "existing file or directory to another name before upgrading "
                        + "to the new release.");
        }
        
        if (checkQuota) {
            verifyMaxComponentLength(child.getLocalNameBytes(), inodes, pos);
            verifyMaxDirItems(inodes, pos);
        }
        
        verifyINodeName(child.getLocalNameBytes());
        
        Quota.Counts counts = child.computeQuotaUsage();
        updateCount(iip, pos, counts.get(Quota.NAMESPACE), counts.get(Quota.DISKSPACE), checkQuota);
        
        boolean isRename = (child.getParent() != null);
        INodeDirectory parent = inodes[pos - 1].asDirectory();
        boolean added;
        try {
            added = parent.addChild(child, true, iip.getLatestSnapshotId());
        } catch(QuotaExceededException e) {
            updateCountNoQuotaCheck(iip, pos, -counts.get(Quota.NAMESPACE), 
                    -counts.get(Quota.DISKSPACE));
            throw e;
        }
        
        if (!added) {
            updateCountNoQuotaCheck(iip, pos, -counts.get(Quota.NAMESPACE),
                    -counts.get(Quota.DISKSPACE));
        } else {
            iip.setINode(pos - 1, child.getParent());
            if (!isRename) {
                AclStorage.copyINodeDefaultAcl(child);
            }
            addToINodeMap(child);
        }
        
        return added;
    }
    
    public final void addToINodeMap(INode inode) {
        if (inode instanceof INodeWithAdditionalFields) {
            inodeMap.put(inode);
        }
    }
    
    private void updateCountNoQuotaCheck(INodesInPath iip, int numOfNodes, long nsDelta, 
            long dsDelta) {
        try {
            updateCount(iip, numOfNodes, nsDelta, dsDelta, false);
        } catch(QuotaExceededException e) {
            logger.error("BUG: unexpected exception", e);
        }
    }
    
    private void updateCount(INodesInPath iip, int numOfINodes, long nsDelta, 
            long dsDelta, boolean checkQuota) throws QuotaExceededException {
        INode[] inodes = iip.getINodes();
        if (numOfINodes > inodes.length) {
            numOfINodes = inodes.length;
        }
        if (checkQuota && !skipQuotaCheck) {
            verifyINodeQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
        }
        unprotectedUpdateCount(iip, numOfINodes, nsDelta, dsDelta);
    }
    
    private void verifyINodeQuota(INode[] inodes, int pos, long nsDelta, 
            long dsDelta, INode commonAncestor) throws QuotaExceededException {
        if (nsDelta <= 0 && dsDelta <= 0) {
            return;
        }
        
        for (int i = (pos > inodes.length ? inodes.length : pos) - 1; i >= 0; i--) {
            if (commonAncestor == inodes[i]) {
                return;
            }
            
            DirectoryWithQuotaFeature q = inodes[i].asDirectory().getDirectoryWithQuotaFeature();
            if (q != null) {
                try {
                    q.verifyQuota(nsDelta, dsDelta);
                } catch (QuotaExceededException e) {
                    e.setPathName(getFullPathName(inodes, i));
                    throw e;
                }
            }
        }
    }
    
    private void unprotectedUpdateCount(INodesInPath iip, int numOfINodes, 
            long nsDelta, long dsDelta) {
        INode[] inodes = iip.getINodes();
        for (int i = 0; i < numOfINodes; i++) {
            if (inodes[i].isQuotaSet()) {
                inodes[i].asDirectory().getDirectoryWithQuotaFeature().addSpaceConsumed2Cache(nsDelta, dsDelta);
            }
        }
    }
    
    private void verifyINodeName(byte[] inodeName) {
        if (Arrays.equals(HdfsConstants.DOT_SNAPSHOT_DIR_BYTES, inodeName)) {
            throw new IllegalArgumentException("Invalid inode name: " + DFSUtil.bytes2String(inodeName));
        }
    }
    
    private void verifyMaxComponentLength(byte[] childName, Object parentPath, int pos) 
            throws PathComponentTooLongException {
        if (maxComponentLength <= 0) {
            return;
        }
        
        int length = childName.length;
        if (length > maxComponentLength) {
            final String p = parentPath instanceof INode[]?
                    getFullPathName((INode[])parentPath, pos - 1): (String)parentPath;
            final PathComponentTooLongException e = new PathComponentTooLongException(
                maxComponentLength, length, p, DFSUtil.bytes2String(childName));
            throw e;
        }
    }
    
    private void verifyMaxDirItems(INode[] inodes, int pos) throws MaxDirectoryItemsExceededException {
        if (maxDirItems <= 0) {
            return;
        }
        
        INodeDirectory parent = inodes[pos - 1].asDirectory();
        int count = parent.getChildrenNum(Snapshot.CURRENT_STATE_ID);
        if (count >= maxDirItems) {
            MaxDirectoryItemsExceededException e = new MaxDirectoryItemsExceededException(maxDirItems, count);
            e.setPathName(getFullPathName(inodes, pos - 1));
            throw e;
        }
    }
    
    private INodesInPath getExistingPathINodes(byte[][] components) throws UnresolvedLinkException {
        return INodesInPath.resolve(rootDir, components);
    }
    
    private static String getFullPathName(INode[] inodes, int pos) {
        StringBuilder builder = new StringBuilder();
        if (inodes[0].isRoot()) {
            if (pos == 0) return Path.SEPARATOR;
        } else {
            builder.append(inodes[0].getLocalName());
        }
        
        for (int i = 0; i <= pos; i++) {
            builder.append(Path.SEPARATOR).append(inodes[i].getLocalName());
        }
        
        return builder.toString();
    }
    
    private final static String DOT_RESERVED_STRING = ".reserved";
    private final static String DOT_RESERVED_PATH_PERFIX = Path.SEPARATOR + DOT_RESERVED_STRING;
    private final static byte[] DOT_RESERVED = DFSUtil.string2Bytes(DOT_RESERVED_STRING);
    private final static String DOT_INODES_STRING = ".inodes";
    private final static byte[] DOT_INODES = DFSUtil.string2Bytes(DOT_INODES_STRING);
    
    static boolean isReservedName(INode inode) {
        return Arrays.equals(inode.getLocalNameBytes(), DOT_RESERVED);
    }
}
