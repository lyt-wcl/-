package com.distributedDb.zk;

public class Server11 {

    /**
     * region1的主服务器
     */
    public static void main(String[] args) throws InterruptedException {
        Thread thread = new Thread(new WorkerRunnable("region1", "region1.1", "db1.1"));
        thread.start();
    }
}
