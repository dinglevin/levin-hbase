package org.levin.hadoop.hdfs.dirs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;

public class LevinFSDirectoryUtil {
    private LevinFSDirectoryUtil() { }
    
    static final byte[] ROOT_NAME = DFSUtil.string2Bytes("");
    
    static INodeDirectorySnapshottable createRoot(LevinFSNamesystem namesystem) {
        INodeDirectory root = new INodeDirectory(INodeId.ROOT_INODE_ID, ROOT_NAME, 
                namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)), 0L);
        
        long nsQuota = DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA;
        long dsQuota = DirectoryWithQuotaFeature.DEFAULT_DISKSPACE_QUOTA;
        root.addDirectoryWithQuotaFeature(nsQuota, dsQuota);
        
        INodeDirectorySnapshottable rootSnapshottable = new INodeDirectorySnapshottable(root);
        return rootSnapshottable;
    }
}
