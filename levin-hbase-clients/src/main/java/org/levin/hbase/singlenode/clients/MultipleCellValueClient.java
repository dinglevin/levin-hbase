package org.levin.hbase.singlenode.clients;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipleCellValueClient {
    private static final Logger logger = LoggerFactory.getLogger(MultipleCellValueClient.class);
    
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/Users/dinglevin/local-lib/hadoop-bin");
        Connection connection = ConnectionFactory.createConnection();
        try {
            setupSchema(connection);
            updateMultiDataToCell(connection);
            
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            
            queryBackForTest(connection);
        } finally {
            connection.close();
        }
    }
    
    private static void setupSchema(Connection connection) {
        try {
            connection.getAdmin().createNamespace(NamespaceDescriptor.create("test").build());
        } catch (IOException e) {
            if (e instanceof NamespaceExistException) {
                logger.info("'test' namespace already exists");
            } else {
                throw new IllegalStateException("Failed to create 'test' namespace", e);
            }
        }
        
        try {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("test:msMultiValues"));
            desc.addFamily(new HColumnDescriptor("data").setMaxVersions(Integer.MAX_VALUE));
            connection.getAdmin().createTable(desc);
        } catch (IOException e) {
            if (e instanceof TableExistsException) {
                logger.info("'msMultiValues' table already exists");
            } else {
                throw new IllegalStateException("Failed to create 'msMultiValues' table", e);
            }
        }
    }
    
    private static void updateMultiDataToCell(Connection connection) throws Exception {
        Table valueTbl = connection.getTable(TableName.valueOf("test:msMultiValues"));
        for (int i = 1; i < 100; i++) {
            Put put = new Put(Bytes.toBytes("rowKey_0"), i);
            put.add(Bytes.toBytes("data"), Bytes.toBytes("column"), Bytes.toBytes("data-" + i));
            valueTbl.put(put);
        }
        logger.info("Update multiple value into one cell completed");
    }
    
    private static void queryBackForTest(Connection connection) throws Exception {
        Table valueTbl = connection.getTable(TableName.valueOf("test:msMultiValues"));
        
        Get get = new Get(Bytes.toBytes("rowKey_0")).setMaxVersions();
        Result result = valueTbl.get(get);
        logResult(result, "Get all data version");
        
        get = new Get(Bytes.toBytes("rowKey_0"));
        get.setTimeStamp(5);
        result = valueTbl.get(get);
        logResult(result, "Get version 5 data");
    }
    
    private static void logResult(Result result, String action) {
        logger.info("Get result for " + action);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (NavigableMap<byte[], NavigableMap<Long, byte[]>> columnsMap : map.values()) {
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : columnsMap.entrySet()) {
                for (Map.Entry<Long, byte[]> entry : qualifierEntry.getValue().entrySet()) {
                    logger.info("Qualifier: " + Bytes.toString(qualifierEntry.getKey()) + " value: " + 
                            Bytes.toString(entry.getValue()) + "[" + entry.getKey() + "]");
                }
            }
        }
    }
}
