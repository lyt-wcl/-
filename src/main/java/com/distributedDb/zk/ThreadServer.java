package com.distributedDb.zk;

public class ThreadServer {

    /**
     * 可以同时打开多个region server。
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        String[] regions = {"region1", "region2", "region2"};
        String[] nodes = {"region1.2", "region2.1", "region2.2"};
        String[] databases = {"mytest2", "db1", "db2"};
        for (int i = 0; i < 3; i++) {
            String regionNode = regions[i];
            String nodeName = nodes[i];
            String database = databases[i];

            Thread thread = new Thread(new WorkerRunnable(regionNode, nodeName, database));
            thread.start();
            Thread.sleep(2000);
        }
    }
}

