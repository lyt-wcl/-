package com.distributedDb.zk;

public class Server21 {

    /**
     * region2的主服务器
     */
    public static void main(String[] args) throws InterruptedException {

        Thread thread = new Thread(new WorkerRunnable("region2", "region2.1", "db2.1"));
        thread.start();

    }
}
