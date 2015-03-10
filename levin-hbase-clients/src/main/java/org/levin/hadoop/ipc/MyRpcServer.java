package org.levin.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyRpcServer {
    public static void main(String[] args) throws Exception {
        Configuration conf = createConf();
        RPC.Builder builder = new RPC.Builder(conf);
        builder.setBindAddress("localhost")
               .setNumHandlers(10)
               .setnumReaders(10)
               .setPort(9001)
               .setQueueSizePerHandler(10)
               .setVerbose(true);
        
        RPC.Server server = builder.build();
        server.start();
    }
    
    private static Configuration createConf() {
        Configuration conf = new Configuration();
        return conf;
    }
}
