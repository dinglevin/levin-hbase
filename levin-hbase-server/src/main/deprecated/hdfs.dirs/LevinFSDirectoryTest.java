package org.levin.hadoop.hdfs.dirs;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class LevinFSDirectoryTest {
    @BeforeClass
    public static void setup() {
        System.setProperty("hadoop.home.dir", "/Users/dinglevin/local-lib/hadoop");
    }
    
    @Test
    public void testCreateOnly() throws Exception {
        System.out.println("<=================testCreateOnly====================>");
        
        LevinFSNamesystem namesystem = new LevinFSNamesystem("levin", "root");
        LevinFSDirectory dir = new LevinFSDirectory(namesystem, new Configuration());
        dir.getRootDir().dumpTreeRecursively(System.out);
        
        System.out.println("<=================testCreateOnly====================>\n");
    }
    
    @Test
    public void testMkDirs() throws Exception {
        System.out.println("<=================testMkDirs====================>");
        
        LevinFSNamesystem namesystem = new LevinFSNamesystem("levin", "root");
        LevinFSDirectory dir = new LevinFSDirectory(namesystem, new Configuration());
        
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/foo", newPerm(),
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/foo/bar", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/home", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/home/levin", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/opt", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/opt/local", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        
        dir.getRootDir().dumpTreeRecursively(System.out);
        
        System.out.println("<=================testMkDirs====================>\n");
    }
    
    @Test
    public void testAddFiles() throws Exception {
        System.out.println("<=================testAddFiles====================>");
        
        LevinFSNamesystem namesystem = new LevinFSNamesystem("levin", "root");
        LevinFSDirectory dir = new LevinFSDirectory(namesystem, new Configuration());
        
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/foo", newPerm(),
                newAclEntries(), System.currentTimeMillis());
        dir.addFile("/foo/file1", newPerm(), (short)3, 1024*1024, "hdfs-client", "hadoop-pig");
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/foo/bar", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/home", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.addFile("/home/file3", newPerm(), (short)3, 1024*1024, "hdfs-client2", "hadoop-eagle");
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/home/levin", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/opt", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/opt/local", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.addFile("/opt/local/file4", newPerm(), (short)3, 1024*1024, "hdfs-client2", "hadoop-eagle");
        dir.addFile("/opt/local/file5", newPerm(), (short)3, 1024*1024, "hdfs-client2", "hadoop-eagle");
        dir.addFile("/opt/local/file6", newPerm(), (short)3, 1024*1024, "hdfs-client2", "hadoop-eagle");
        dir.addFile("/opt/local/file7", newPerm(), (short)3, 1024*1024, "hdfs-client2", "hadoop-eagle");
        
        dir.getRootDir().dumpTreeRecursively(System.out);
        
        System.out.println("<=================testAddFiles====================>\n");
    }
    
    @Test
    public void testWriteToFile() throws Exception {
        System.out.println("<=================testWriteToFile====================>");
        
        LevinFSNamesystem namesystem = new LevinFSNamesystem("levin", "root");
        LevinFSDirectory dir = new LevinFSDirectory(namesystem, new Configuration());

        dir.unprotectedMkdir(namesystem.nextINodeId(), "/opt", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        dir.unprotectedMkdir(namesystem.nextINodeId(), "/opt/local", newPerm(), 
                newAclEntries(), System.currentTimeMillis());
        
        dir.addFile("/opt/local/test.txt", newPerm(), (short)3, 1024*1024, "hdfs-client2", "hadoop-eagle");

        BlockInfo b1 = dir.addBlock("/opt/local/test.txt", createDatanodeStorages());
        b1.setNumBytes(1024*1024);
        namesystem.getBlockManager().commitBlock(b1);
        dir.updateSpaceConsumed("/opt/local/test.txt", 0, 1024*1024);
        BlockInfo b2 = dir.addBlock("/opt/local/test.txt", createDatanodeStorages());
        b2.setNumBytes(1024);
        namesystem.getBlockManager().commitBlock(b2);
        dir.updateSpaceConsumed("/opt/local/test.txt", 0, 1024);
        
        dir.completeFile("/opt/local/test.txt");
        
        dir.getRootDir().dumpTreeRecursively(System.out);
        
        System.out.println("<=================testWriteToFile====================>\n");
    }
    
    private static PermissionStatus newPerm() {
        return new PermissionStatus("ding", "hadoop", new FsPermission((short)0755));
    }
    
    private static List<AclEntry> newAclEntries() {
        AclEntry entry1 = new AclEntry.Builder().setType(AclEntryType.USER)
                .setName("tom").setScope(AclEntryScope.ACCESS).setPermission(FsAction.READ_WRITE).build();
        AclEntry entry2 = new AclEntry.Builder().setType(AclEntryType.GROUP)
                .setName("group1").setScope(AclEntryScope.ACCESS).setPermission(FsAction.READ).build();
        AclEntry entry3 = new AclEntry.Builder().setType(AclEntryType.MASK)
                .setName("acb").setScope(AclEntryScope.ACCESS).setPermission(FsAction.READ).build();
        AclEntry entry4 = new AclEntry.Builder().setType(AclEntryType.OTHER)
                .setName("other").setScope(AclEntryScope.ACCESS).setPermission(FsAction.NONE).build();
        return Lists.newArrayList(entry1, entry2, entry3, entry4);
    }
    
    private static DatanodeStorageInfo[] createDatanodeStorages() {
        DatanodeStorageInfo[] infos = new DatanodeStorageInfo[1];
        DatanodeStorage storage = new DatanodeStorage(DatanodeStorage.generateUuid());
        DatanodeDescriptor descriptor = new DatanodeDescriptor(new DatanodeID("127.0.0.1", "localhost", 
                DataNode.generateUuid(), 8001, 8002, 8003, 8004));
        infos[0] = new DatanodeStorageInfo(descriptor, storage);
        return infos;
    }
}
