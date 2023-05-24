package com.distributedDb.zk;

public class Server22 {

    /**
     * region2的副服务器
     */
    public static void main(String[] args) throws InterruptedException {

        Thread thread = new Thread(new WorkerRunnable("region2", "region2.2", "db2.2"));
        thread.start();

    }
}
