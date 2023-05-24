package com.distributedDb.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.distributedDb.zk.DbOperations.*;

/**
 * 一个region 服务器类，主要功能有连接数据库，连接zookeeper集群，监听特定节点，返回sql语句执行结果。辅助Master实现容错容灾
 */
public class DistributeServer {
    private String connectionString = "192.168.10.102:2181";
    private int sessionTimeout = 2000;
    private String userName = "root";
    private String password = "lyt3817081";

    /**
     * 服务器名字
     */
    private String serverName;

    /**
     * 所在的region节点名
     */
    private String regionNode;

    /**
     * 与数据库的连接
     */
    private Connection connection;
    private ZooKeeper zkClient;


    /**
     * 一个region 服务器的执行逻辑，主要功能有连接数据库，连接zookeeper集群，监听特定节点，返回sql语句执行结果。辅助Master实现容错容灾
     * @param args 传入的参数，按顺序分别是服务器名，region节点名和要连接的数据库
     */
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException, SQLException {
        DistributeServer server = new DistributeServer();
        String database;
        if (args.length < 3) {
            server.regionNode = "region1";
            server.serverName = "region1.1";
            database = "mytest";
        } else {
            server.regionNode = args[0];
            server.serverName = args[1];
            database = args[2];
        }
        String data = "xxx.xxx.xxx";
        if(!server.regionNode.startsWith("region")){
            System.out.printf(server.regionNode + "命名不规范");

        }
        // 连接数据库
        server.connectDatabase(database, server.userName, server.password);

        //连接zookeeper集群
        server.connectZookeeper(server.serverName, server.regionNode, data);
        server.getSqlStatement(server.regionNode);
        server.getSelfData();

        Thread.sleep(5000);
        server.keepTwoServers();
        server.sendCopyRequest();


        server.business();

    }

    /**
     * 连接数据库
     * @param database:数据库名
     * @param user:用户名
     * @param password:密码
     */
    private void connectDatabase(String database, String user, String password) {
        try{
            connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/" + database, user, password);
        } catch (SQLException e) {
            System.out.printf("服务器连接失败！！！");
            System.exit(0);
        }
    }

    /**
     * 连接zookeeper集群，并在自己的region下面注册一个节点
     */
    private void connectZookeeper(String nodeName, String regionNode,String IP) throws IOException, InterruptedException, KeeperException {
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });

        Stat stat = zkClient.exists("/master/" + regionNode, false);
        if(stat == null) {
            String path = zkClient.create
                    ("/master/" + regionNode , "".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        stat = zkClient.exists("/master/" + regionNode + "/" + nodeName, false);
        if(stat == null) {
            String path = zkClient.create
                    ("/master/" + regionNode + "/" + nodeName, IP.getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(path);
        } else {
            System.out.printf("this server already exists");
        }

    }


    /**
     * 保持服务器一直在线
     */
    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 监听自身节点，根据里面的值判断是否要发送数据库信息给副服务器
     */
    private void getSelfData() throws InterruptedException, KeeperException {
        zkClient.getData("/master/" + regionNode + "/" + serverName, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                byte[] data = new byte[0];
                try {
                    Stat stat = zkClient.exists("/master/" + regionNode + "/" + serverName, null);
                    if(stat!=null) {
                        data = zkClient.getData("/master/" + regionNode + "/" + serverName, this, null);
                    } else {
                        System.out.println(1);
                    }
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                String str = new String(data);
                if(str.equals("need copy db")) {
                    try {
                        sendDbData();
                    } catch (SQLException | InterruptedException | KeeperException e) {
                        throw new RuntimeException(e);
                    }
                } else{
//                    System.out.println(str);
                    try {
                        copyDb(str);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
//
            }
        },null);
    }

    /**
     * 复制主服务器数据库的所有表格
     * @param combinedSQLString 主服务器发送的所有表格的DDL语句和select * 结果
     */
    private void copyDb(String combinedSQLString) throws SQLException {

        String[] parts = combinedSQLString.split("#SELECT##");
        if(parts.length >=3){
            String[] tableNames = parts[0].split("#");
            String[] createStatements = parts[1].split("#");
            String[] selectResults = parts[2].split("SELECT##");
            Statement statement = connection.createStatement();
            List<String>selfTableList = getTableNames(connection);
            for(String tableName: selfTableList) {
                String sql = "drop table " + tableName;
                statement.executeUpdate(sql);
            }
            for (String createStatement : createStatements) {
                statement.executeUpdate(createStatement);
            }
            int index = 0;
            for(String selectResult : selectResults) {
                List<Map<String, Object>> tableData = new ArrayList<>();
                String[] lines = selectResult.split("\n");
                for (String line : lines) {
                    Map<String, Object> row = new HashMap<>();
                    String[] keyValuePairs = line.split(", ");
                    for (String keyValuePair : keyValuePairs) {
                        String[] keyValue = keyValuePair.split(": ");
                        String key = keyValue[0].trim();
                        Object value = parseValue(keyValue[1].trim()); // 解析值
                        row.put(key, value);
                    }
                    tableData.add(row);
                }
                insertTableData(connection, tableNames[index], tableData);
                index++;
            }
            statement.close();
        }
        else {
            System.out.println(combinedSQLString);
        }

    }

    /**
     * 将字符串转换为int或者float
     */
    private Object parseValue(String valueStr) {
        Object value;

        // 尝试解析为整数
        try {
            value = Integer.parseInt(valueStr);
            return value;
        } catch (NumberFormatException e) {
            // 解析失败，尝试其他类型
        }

        // 尝试解析为浮点数
        try {
            value = Double.parseDouble(valueStr);
            return value;
        } catch (NumberFormatException e) {
            // 解析失败，将值保持为字符串
        }

        // 值无法解析为整数或浮点数，将其保持为字符串
        value = valueStr;
        return value;
    }

    /**
     * 主服务器向副服务器发送对应的DDL和和select * 结果，以便于副服务器实现数据库复制
     */
    private void sendDbData() throws SQLException, InterruptedException, KeeperException {
        List<String> tableNames = getTableNames(connection);
        StringBuilder sb = new StringBuilder();
        for(String tableName : tableNames) {
            sb.append(tableName);
            sb.append("#");
        }
        sb.append("SELECT##");
        for(String tableName : tableNames) {
            String showSql = "SHOW CREATE TABLE " + tableName;
            sb.append(runShow(connection,showSql));
            sb.append("#");
        }
        for(String tableName : tableNames) {
            String selectSql = "SELECT * FROM " + tableName;
            sb.append(runSelect(connection, selectSql));
        }
        zkClient.setData("/master/" + regionNode + "/" + regionNode + ".2", sb.toString().getBytes(StandardCharsets.UTF_8), -1);
    }

    /**
     * 副服务器刚刚上线时调用，向主服务器发送复制数据库请求
     */
    private void sendCopyRequest() throws InterruptedException, KeeperException {
        if(isPrimaryServer()) {
            return;
        }
        else {
            zkClient.setData("/master/" + regionNode + "/" + regionNode + ".1", "need copy db".getBytes(), -1);
        }
    }

    /**
     * 根据服务器的名字来确认是不是主服务器，只有“.”的后面只有1的就返回true，否则返回false
     * 副服务器不需要执行select操作，同时执行非select操作，不返回结果
     * @return true表示主服务器，false表示副服务器
     */
    private boolean isPrimaryServer() {
        int dotIndex = serverName.indexOf(".");
        if (dotIndex != -1 && dotIndex < serverName.length() - 1) {
            String afterDot = serverName.substring(dotIndex + 1);
            if (afterDot.equals("1")) {
                return true;
            }
        }
        return false;
    }

    /**
     * 保证同一个region节点下有两个服务器，一主一副。若没有则向master发送唤起空闲服务器的请求
     */
    private void keepTwoServers() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/master/" + regionNode, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    List<String> children = zkClient.getChildren("/master/" + regionNode, this);
                    if(children.size() < 2) {
                        evokeServer();
                    }
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        if(children.size() < 2) {
            evokeServer();
        }
    }

    /**
     * 将消息发给master，让它唤醒一个新的服务器。同时如果自己是副本机，那就将自己变为主机，同时改名zookeeper里面的节点。
     */
    private void evokeServer() throws InterruptedException, KeeperException {
        if(!isPrimaryServer()) {
            zkClient.delete("/master/" + regionNode + "/" + serverName, -1);
            serverName = regionNode + ".1";
            zkClient.create("/master/" + regionNode + "/" + serverName, "".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        while (true) {
            Stat stat = zkClient.exists("/master/" + regionNode + "/" + regionNode + ".2", null);
            if(stat == null && isPrimaryServer())
                break;
            else {
                Thread.sleep(1000);
                System.out.println(1);
            }
        }
        zkClient.setData("/master/evokeServer", regionNode.getBytes(), -1);
    }

    /**
     * 监听自己的父节点（region节点），获取sql语句，并执行相应的操作
     * @param regionNode region节点名
     */
    private void getSqlStatement(String regionNode) throws InterruptedException, KeeperException {
        zkClient.getData("/master/" + regionNode, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                byte[] data;
                try {
                    data = zkClient.getData("/master/" + regionNode, this, null);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //获取到的字符串由3部分组成，它们由“##”表示分隔
                String combinedSQLString = new String(data);
                String[] parts = combinedSQLString.split("##");
                String targetNode = parts[0];//第一部分返回的目标节点
                String operationType = parts[1];//第二部分是sql操作的类型，主要是区分select操作和其他操作
                String sqlStatement = parts[2];//第三部分是执行语句

                if(operationType.equals("SELECT")) {
                    if(isPrimaryServer()){
                        try {
                            String queryResult = runSelect(connection, sqlStatement);
                            setExecuteSqlResult(queryResult, targetNode);
                        } catch (UnsupportedEncodingException | InterruptedException | KeeperException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        //副服务器不需要执行任何select操作
                    }

                } else if (operationType.equals("SHOW")) {
                    String[] statements = sqlStatement.split("#");
                    System.out.println(sqlStatement + statements.length);
                    String showSql = statements[0];
                    String selectSql = statements[1];
                    String showResult = runShow(connection, showSql);
                    String queryResult = runSelect(connection, selectSql);
                    try {
                        if(isPrimaryServer()) {
                            setExecuteSqlResult(showResult + queryResult, targetNode);
                        }
                    } catch (UnsupportedEncodingException | InterruptedException | KeeperException e) {
                        throw new RuntimeException(e);
                    }

                } else {
                    try {
                        String updateResult = runUpdate(connection, sqlStatement, operationType);
                        if(isPrimaryServer()) {
                            setExecuteSqlResult(updateResult, targetNode);
                        }

                    } catch (UnsupportedEncodingException | InterruptedException | KeeperException e) {
                        throw new RuntimeException(e);
                    }
                }


            }
        }, null);
    }

    /**
     * 根据目标节点名，将结果（字符串）存入对应的节点
     * @param sqlStatement sql语句执行结果
     * @param targetNode 目标节点名
     */
    private void setExecuteSqlResult(String sqlStatement, String targetNode) throws InterruptedException, KeeperException, UnsupportedEncodingException {
        String path = "/master/" + targetNode;
        byte[] data = sqlStatement.getBytes(StandardCharsets.UTF_8);
        zkClient.setData(path, data, -1);
    }

}
