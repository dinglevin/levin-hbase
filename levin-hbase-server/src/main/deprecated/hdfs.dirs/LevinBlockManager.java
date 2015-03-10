package org.levin.hadoop.hdfs.dirs;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlocksMap;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap;

public class LevinBlockManager {
    private BlocksMap blocksMap = new BlocksMap(1024 * 1024);
    private CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();
    
    public BlockInfo addBlockConllection(BlockInfo block, BlockCollection bc) {
        return blocksMap.addBlockCollection(block, bc);
    }

    public void commitBlock(BlockInfo b1) throws IOException {
        BlockCollection bc = blocksMap.getBlockCollection(b1);
        ((BlockInfoUnderConstruction) b1).commitBlock(b1);
        BlockInfo binfo = ((BlockInfoUnderConstruction) b1).convertToCompleteBlock();
        BlockInfo[] binfos = bc.getBlocks();
        for (int i = 0; i < binfos.length; i++) {
            if (binfos[i] == b1) {
                bc.setBlock(i, binfo);
            }
        }
    }

    public void removeBlockFromMap(Block block) {
        blocksMap.removeBlock(block);
        corruptReplicas.removeFromCorruptReplicasMap(block);
    }
}
