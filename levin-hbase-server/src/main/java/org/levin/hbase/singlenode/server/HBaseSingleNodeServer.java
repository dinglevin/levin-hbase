package org.levin.hbase.singlenode.server;

import org.apache.hadoop.hbase.master.HMaster;

public class HBaseSingleNodeServer {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/Users/dinglevin/local-lib/hadoop-bin");
        HMaster.main(new String[] { "start" });
    }
}
