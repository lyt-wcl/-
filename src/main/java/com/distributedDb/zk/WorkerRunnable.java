package com.distributedDb.zk;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.sql.SQLException;

class WorkerRunnable implements Runnable {
    private String regionNode;
    private String nodeName;
    private String database;

    public WorkerRunnable(String regionNode, String nodeName, String database) {
        this.regionNode = regionNode;
        this.nodeName = nodeName;
        this.database = database;
    }

    @Override
    public void run() {
        try {
            DistributeServer.main(new String[]{regionNode, nodeName, database});
        } catch (IOException | InterruptedException | KeeperException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}