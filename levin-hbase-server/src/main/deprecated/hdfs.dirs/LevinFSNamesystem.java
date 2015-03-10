package org.levin.hadoop.hdfs.dirs;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.SequentialBlockIdGenerator;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.util.SequentialNumber;

public class LevinFSNamesystem {
    private String supergroup;
    private String fsOwner;
    
    private LevinBlockManager blockManager;
    private SequentialNumber blockIdGenerator;
    
    private volatile long inodeId = INodeId.ROOT_INODE_ID;
    
    public LevinFSNamesystem(String fsOwner, String supergroup) {
        this.fsOwner = fsOwner;
        this.supergroup = supergroup;
        
        this.blockManager = new LevinBlockManager();
        this.blockIdGenerator = new SequentialNumber(
                SequentialBlockIdGenerator.LAST_RESERVED_BLOCK_ID) {};
    }
    
    public LevinBlockManager getBlockManager() {
        return blockManager;
    }
    
    public Block createNewBlock() throws IOException {
        Block b = new Block(nextBlockId(), 0, 0);
        b.setGenerationStamp(System.currentTimeMillis());
        return b;
    }
    
    private long nextBlockId() throws IOException {
        return blockIdGenerator.nextValue();
    }
    
    public PermissionStatus createFsOwnerPermissions(FsPermission perm) {
        return new PermissionStatus(fsOwner, supergroup, perm);
    }
    
    public long nextINodeId() {
        return ++inodeId;
    }

    public void incrDeletedFileCount(int i) {
        // TODO Auto-generated method stub
        
    }

    public void removePathAndBlocks(String src,
            BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
            boolean b) {
        // TODO Auto-generated method stub
        
    }

    public void removeSnapshottableDirs(
            List<INodeDirectorySnapshottable> snapshottableDirs) {
        // TODO Auto-generated method stub
        
    }

    public void unprotectedChangeLease(String src, String dst) {
        // TODO Auto-generated method stub
        
    }
}
