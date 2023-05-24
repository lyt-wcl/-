package com.distributedDb.zk;

public class Server12 {

    /**
     * region1的副服务器
     */
    public static void main(String[] args) throws InterruptedException {
        Thread thread = new Thread(new WorkerRunnable("region1", "region1.2", "db1.2"));
        thread.start();
    }
}
