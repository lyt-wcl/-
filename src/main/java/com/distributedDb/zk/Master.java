package com.distributedDb.zk;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.*;

import static com.distributedDb.zk.DbOperations.*;

/**
 * Master类，实现负载均衡，实现容错容灾，辅助实现复杂查询
 */
public class Master {

    /**
     * 连接zookeeper的IP地址
     */
    private String connectionString = "192.168.10.102:2181";
    private static final String MASTER_NODE = "/master";

    private String userName = "root";
    private String password = "lyt3817081";
    private int sessionTimeout = 4000;

    /**
     * 存储着数据表和对应region的映射
     */
    private final Map<String, String> tableRegionMap = new HashMap<>();

    /**
     * 存储着region和其表的数量的映射
     */
    private Map<String, Integer> regionTableNum = new TreeMap<>();

    /**
     * 闲置的数据库列表
     */
    private List<String> idleDb = new ArrayList<>();

    /**
     * 最近被使用过的表。其值越小表示越近被使用过
     */
    private Map<String, Integer> tableLastUsedMap = new HashMap<>();

    /**
     * 当前使用的闲置数据库数量
     */
    int dbIndex = 0;

    /**
     * 闲置数据库总数量
     */
    int idleDbNum = 2;

    /**
     * master最多可以储存的表的数量，可以修改
     */
    int maxTableNum = 3;

    /**
     * 与数据库的连接
     */
    private Connection connection;
    private ZooKeeper zkClient;


    /**
     * 实现负载均衡，实现容错容灾，辅助实现复杂查询
     */
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException, SQLException {
        Master master = new Master();
        master.connectDatabase("master_db", master.userName, master.password);
        master.connectZookeeper();
        master.retrieveMapFromZooKeeper();
        master.initRegionTableNum();
        master.initTableLastUsedMap();
        master.idleDb.addAll(Arrays.asList("idledb1", "idledb2"));

        master.masterSql("masterSql");

        master.evokeServer("evokeServer");

        Thread.sleep(Long.MAX_VALUE);

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
        }

    }

    /**
     * 连接zookeeper集群
     */
    private void connectZookeeper() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        }, null);
    }

    /**
     * 当执行create或者drop时，需要更新tableRegionMap,并将值存储到/master节点
     */
    private void storeMapToZooKeeper() {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            // 构建要存储的字符串，格式为 key1->value1;key2->value2;...
            for (Map.Entry<String, String> entry : tableRegionMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                stringBuilder.append(key).append("->").append(value).append(";");
            }

            // 将字符串存储到ZooKeeper的/master节点
            String path = "/master";
            byte[] data = stringBuilder.toString().getBytes("UTF-8");
            zkClient.setData(path, data, -1);
            System.out.println("Map stored in ZooKeeper successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从ZooKeeper的/master节点获取存储的字符串，解析字符串并将键值对存储到tableRegionMap中
     */
    private void retrieveMapFromZooKeeper() {
        try{
            // 从ZooKeeper的/master节点获取存储的字符串
            byte[] data = zkClient.getData(MASTER_NODE, null, null);
            String storedData = new String(data);

            // 解析字符串并将键值对存储到Map中
            String[] keyValuePairs = storedData.split(";");
            for (String keyValuePair : keyValuePairs) {
                String[] parts = keyValuePair.split("->");
                if (parts.length == 2) {
                    String key = parts[0];
                    String value = parts[1];
                    tableRegionMap.put(key, value);
                }
            }
            System.out.println("Map retrieved from ZooKeeper successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 监听/master/masterSql, 里面的值是master需要执行的sql操作。
     * @param node 监听的节点名
     */
    private void masterSql(String node)throws InterruptedException, KeeperException {
        zkClient.getData("/master/" + node, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                byte[] data;
                try {
                    data = zkClient.getData("/master/" + node, this, null);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //拆分combinedSQLString
                String combinedSQLString = new String(data);
                System.out.println(combinedSQLString);
                String[] parts = combinedSQLString.split("##");
                String tableName = parts[0];
                String clientName = parts[1];
                String operationType = parts[2];
                String sqlStatement = parts[3];
                String targetRegion = "";

                //当sql是create时，master需要先根据负载均衡找到对应的region
                //然后将对应的sql语句以及一些必要的内容发送给目标region
                //同时更新tableRegionMap
                if(operationType.equals("CREATE_TABLE")) {
                    targetRegion = findMinNumRegion();
                    Integer value = regionTableNum.get(targetRegion);
                    regionTableNum.put(targetRegion, value + 1);
                    tableRegionMap.put(tableName, targetRegion);
                    storeMapToZooKeeper();
                    combinedSQLString = clientName + "##" + operationType + "##" + sqlStatement;
                    try {
                        setSqlStatement(combinedSQLString, targetRegion);
                    } catch (InterruptedException | KeeperException | UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                } else if (operationType.equals("DROP_TABLE")){
                    if(tableLastUsedMap.containsKey(tableName)) {
                        try {
                            Statement statement = connection.createStatement();
                            statement.executeUpdate(sqlStatement);
                            statement.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    targetRegion = tableRegionMap.get(tableName);
                    Integer value = regionTableNum.get(targetRegion);
                    regionTableNum.put(targetRegion, value - 1);
                    tableRegionMap.remove(tableName);
                    storeMapToZooKeeper();
                    combinedSQLString = clientName + "##" + operationType + "##" + sqlStatement;
                    try {
                        setSqlStatement(combinedSQLString, targetRegion);
                    } catch (InterruptedException | KeeperException | UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                } else if (operationType.equals("INSERT") | operationType.equals("DELETE") | operationType.equals("UPDATE")){
                    if(tableLastUsedMap.containsKey(tableName)){
                        try {
                            Statement statement = connection.createStatement();
                            statement.executeUpdate(sqlStatement);
                            statement.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                } else {
                    try {
                        sendCreateData(tableName, sqlStatement, clientName);
                    } catch (InterruptedException | KeeperException | UnsupportedEncodingException | SQLException e) {
                        throw new RuntimeException(e);
                    }

                }


            }
        }, null);
    }

    /**
     * 当需要复制数据库时，往对应的region发送消息
     * @param combineTableName 需要复制的数据表名列表
     * @param sqlStatement 执行的sql语句
     * @param clientName 发送该语句的客户名
     */
    private void sendCreateData(String combineTableName, String sqlStatement, String clientName) throws InterruptedException, KeeperException, UnsupportedEncodingException, SQLException {
        String[] tableNames = combineTableName.split(",");
        boolean haveAllTables = true;
        for(String name : tableNames) {
            if(!tableLastUsedMap.containsKey(name)){
                String regionNode = tableRegionMap.get(name);
                Stat stat;
                stat = zkClient.exists("/master/masterSql/" + regionNode, false);
                if(stat == null) {
                    zkClient.create("/master/masterSql/" + regionNode , "".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                String query = "SHOW CREATE TABLE " + name + "#" + "select * from " + name;
                String  combinedSQLString = "masterSql/"+ regionNode + "##" + "SHOW" + "##" + query;
                createTable("masterSql/"+ regionNode, name, sqlStatement, clientName);
                setSqlStatement(combinedSQLString, regionNode);
                haveAllTables = false;
            }

        }
        if(haveAllTables) {
            String queryResult = runSelect(connection, sqlStatement);
            try {
                zkClient.setData("/master/" + clientName, queryResult.getBytes(), -1);
            } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        lruMap(combineTableName);

    }

    /**
     * 根据接收到的region数据复制对应的数据库
     * @param node 监听的region节点
     * @param tableName 需要复制的表名
     * @param sqlStatement 需要执行的sql语句
     * @param clientName 发送该语句的客户名
     * @throws InterruptedException
     * @throws KeeperException
     * @throws SQLException
     */
    private void createTable(String node, String tableName, String sqlStatement, String clientName) throws InterruptedException, KeeperException,SQLException {
        zkClient.getData("/master/" + node, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                byte[] data;
                try {
                    data = zkClient.getData("/master/" + node, false, null);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                String combinedSQLString = new String(data);
                String[] parts = combinedSQLString.split("SELECT##");
                String showResult = parts[0];
                String selectResult = parts[1];
                Statement statement;
                try {
                    statement = connection.createStatement();
                    statement.executeUpdate(showResult);
                    System.out.println("表已创建");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

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
                try {
                    insertTableData(connection, tableName, tableData);
                    System.out.println("数据已插入");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                String queryResult = runSelect(connection, sqlStatement);
                try {
                    zkClient.setData("/master/" + clientName, queryResult.getBytes(), -1);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }, null);
    }

    /**
     * 监听特定节点，当触发事件时，唤起闲置服务器
     * @param node 此处是evokeServer节点
     */
    private void evokeServer(String node) throws InterruptedException, KeeperException {
        zkClient.getData("/master/" + node, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                byte[] data;
                try {
                    data = zkClient.getData("/master/" + node, false, null);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                String regionNode = new String(data);
                if(regionNode.startsWith("region")) {
                    System.out.println(regionNode);
                    List<String> children;
                    try {
                        children = zkClient.getChildren("/master/" + regionNode,false);
                    } catch (KeeperException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    if(children.size() < 2){
                        if(dbIndex < idleDbNum) {
                            Thread thread = new Thread(new WorkerRunnable(regionNode, regionNode + ".2", idleDb.get(dbIndex)));
                            dbIndex++;
                            thread.start();
                        }
                        else {
                            System.out.println("闲置服务器不够");
                        }
                    }
                    try {
                        zkClient.setData("/master/" + node, "".getBytes(), -1);
                    } catch (KeeperException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // 重新注册监听
                try {
                    evokeServer(node);
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }, null);
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
     * 设置对应节点的sql语句和必要的信息
     * @param SqlStatement 需要传递的sql语句和必要的信息
     * @param regionNode 目标节点
     */
    private void setSqlStatement(String SqlStatement, String regionNode) throws InterruptedException, KeeperException, UnsupportedEncodingException {
        String path = "/master/" + regionNode;
        byte[] data = SqlStatement.getBytes("UTF-8");
        zkClient.setData(path, data, -1);
    }

    /**
     * 初始化RegionTableNum
     */
    private void initRegionTableNum() throws InterruptedException, KeeperException {
        List<String> regions = zkClient.getChildren("/master", false);
        for(String region : regions) {
            if(region.startsWith("region")) {
                regionTableNum.put(region, 0);
            }
        }
        for(String region : tableRegionMap.values()) {
            Integer value = regionTableNum.get(region);
            regionTableNum.put(region, value + 1);
        }
    }

    /**
     * 读取/master节点的值，初始化TableLastUsedMap
     */
    private void initTableLastUsedMap() throws SQLException {
        List<String> tableList = getTableNames(connection);
        for(String tableName : tableList) {
            tableLastUsedMap.put(tableName, 1);
        }
    }

    /**
     * LRU算法，用于处理当数据库表格过多时，选择删除哪一种表
     * @param combineTableName 此次使用到的表名
     */
    private void lruMap(String combineTableName) throws SQLException {
        String[] tableNames = combineTableName.split(",");
        for(String tableName : tableNames) {
            tableLastUsedMap.put(tableName, 0);
        }
        for(Map.Entry<String, Integer> entry : tableLastUsedMap.entrySet()) {
            tableLastUsedMap.put(entry.getKey(), entry.getValue() + 1);

        }
        evictIfNeeded();
    }

    /**
     * LRU的辅助算法
     */
    private void evictIfNeeded() throws SQLException {
        while (tableLastUsedMap.size() > maxTableNum) {
            Integer masNum = 0;
            String uselessTable = "";
            for(Map.Entry<String, Integer> entry : tableLastUsedMap.entrySet()) {
                if(entry.getValue() > masNum) {
                    masNum = entry.getValue();
                    uselessTable = entry.getKey();
                }
            }
            tableLastUsedMap.remove(uselessTable);
            Statement statement = connection.createStatement();
            String dropSql = "drop table " + uselessTable;
            statement.executeUpdate(dropSql);
            statement.close();

        }
    }

    /**
     * 找到存储表数量最少的region
     * @return 存储表数量最少的region名
     */
    private String findMinNumRegion() {
        Integer min = Integer.MAX_VALUE;
        String target = "";
        for(Map.Entry<String, Integer> entry : regionTableNum.entrySet()) {
            if(entry.getValue() <= min) {
                target = entry.getKey();
                min = entry.getValue();
            }

        }
        return target;
    }

}
