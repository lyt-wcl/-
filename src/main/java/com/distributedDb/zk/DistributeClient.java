package com.distributedDb.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.io.IOException;
import org.tinygroup.jsqlparser.statement.Statement;
import org.tinygroup.jsqlparser.statement.drop.Drop;
import org.tinygroup.jsqlparser.statement.insert.Insert;
import org.tinygroup.jsqlparser.statement.delete.Delete;
import org.tinygroup.jsqlparser.statement.create.table.CreateTable;
import org.tinygroup.jsqlparser.statement.update.Update;
import org.tinygroup.jsqlparser.util.TablesNamesFinder;
import org.tinygroup.jsqlparser.JSQLParserException;
import org.tinygroup.jsqlparser.parser.CCJSqlParserUtil;
import org.tinygroup.jsqlparser.statement.select.Select;

/**
 * 客户端类，用户输入sql语句，客户端处理后，
 */
public class DistributeClient {

    /**
     * 连接zookeeper的IP地址
     */
    private String connectionString = "192.168.10.102:2181";
    private static final String MASTER_NODE = "/master";

    /**
     * 客户名，方便服务器将结果返回
     */
    private String clientName;

    /**
     * 存储着数据表和对应region的映射
     */
    private final Map<String, String> tableRegionMap = new HashMap<>();
    private int sessionTimeout = 4000;
    private ZooKeeper zkClient;
    private static final String prefix = "distributed_minisql> ";
    private static final String morelines = "                  -> ";
    private static final String farewell = "Bye";

    /**
     * 分布式数据库的客户端，用户输入sql语句，客户端处理后，
     * 将sql语句发给对应的节点。根据监听的特定节点，
     * 获取sql语句的执行结果并打印出来
     */
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        DistributeClient client = new DistributeClient();

        client.getConnect();

        client.retrieveMapFromZooKeeper();

        client.getExecuteSqlResult(client.clientName);
        Scanner scanner = new Scanner(System.in);
        StringBuilder sb = new StringBuilder();
        boolean isMoreLines = false;
        System.out.print(prefix);
        while (true) {
            String line = scanner.nextLine();
            if (!isMoreLines && line.equals("quit")) {
                System.out.println(farewell);
                scanner.close();
                break;
            }
            if (line.endsWith(";")) {
                sb.append(line, 0, line.length());
                String result = client.process(sb.toString());
                if(result.equals("success"))
                    Thread.sleep(1000);
                else
                    System.out.println(result);
                System.out.print(prefix);
                sb.setLength(0);
                isMoreLines = false;
            } else {
                isMoreLines = true;
                sb.append(line);
                System.out.print(morelines);
            }
        }
    }

    /**
     * 输入自己的用户名，并且在/master/client节点下注册自己的节点
     */
    private void inputClientName() throws InterruptedException, KeeperException {
        Thread.sleep(1000);
        Stat stat;
        do {
            System.out.println("please input your name.");
            Scanner scanner = new Scanner(System.in);
            clientName = scanner.nextLine();
            stat = zkClient.exists("/client/" + clientName, false);
            if(stat != null) {
                System.out.println("Customer name already exists.");
            } else {
                zkClient.create
                        ("/master/client/" + clientName, "1".getBytes(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
        } while (stat != null);
    }

    /**
     * 识别用户输入是否合法，以及根据sql类型选择接下来的操作逻辑
     * @param input 输入的字符串
     * @return 若sql语句不合法，则返回提示。反之返回success
     */
    private String process(String input) throws UnsupportedEncodingException, InterruptedException, KeeperException {
        Statement statement;
        String table_name;
        String operationType;
        List<String> tableList = new ArrayList<>();
        if(input.contains("SHOW") || input.contains("show")) {
            for(String tableName: tableRegionMap.keySet()){
                System.out.println(tableName);
            }
            return "success";
        }
        try {
            statement = CCJSqlParserUtil.parse(input);
        } catch (JSQLParserException e) {
            return "You have an error in your SQL syntax.";
        }
        if (statement instanceof CreateTable) {
            // get the table name of statement
            CreateTable createTableStatement = (CreateTable) statement;
            table_name = createTableStatement.getTable().getName();

            if(tableRegionMap.containsKey(table_name)) {
                return "Table " + table_name + " already exists.";
            } else {
                operationType = "CREATE_TABLE";
            }
            tableList.add(table_name);
        } else if (statement instanceof Drop) {
            // get the table name of statement
            Drop dropStatement = (Drop) statement;
            table_name = dropStatement.getName();
            if(!tableRegionMap.containsKey(table_name)) {
                return "Table " + table_name + " doesn't exist.";
            } else {
                operationType = "DROP_TABLE";
            }
            tableList.add(table_name);
        } else if (statement instanceof Select) {
            Select selectStatement = (Select) statement;

            // get the tables names of statement
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            tableList = tablesNamesFinder.getTableList(selectStatement);
            for (String name : tableList) {
                // check if the table exists
                if (!tableRegionMap.containsKey(name)) {
                    return "Table " + name + " doesn't exist.";
                }
            }
            operationType = "SELECT";
        } else if (statement instanceof Delete) {
            // get the table name of statement
            Delete deleteStatement = (Delete) statement;
            table_name = deleteStatement.getTable().getName();
            tableList.add(table_name);
            operationType = "DELETE";
        } else if (statement instanceof Insert) {
            // get the table name of statement
            Insert insertStatement = (Insert) statement;
            table_name = insertStatement.getTable().getName();
            tableList.add(table_name);
            operationType = "INSERT";
        } else if (statement instanceof Update) {
            // get the table name of statement
            Update updateStatement = (Update) statement;
            table_name = updateStatement.getTables().get(0).getName();
            tableList.add(table_name);
            operationType = "UPDATE";
        } else {
            return "The function is not supported.";
        }
        // execute the SQL
        String sqlStatement = input;

        //当需要查询或者操作的表只有一张，对应的逻辑。
        if(tableList.size() == 1) {
            String name = tableList.get(0);
            String combinedSQLString = "client/" + clientName + "##" + operationType + "##" + sqlStatement;
            if(operationType.equals("SELECT")){
                String regionNode =tableRegionMap.get(name);
                setSqlStatement(combinedSQLString, regionNode);
            } else if(operationType.equals("CREATE_TABLE") || operationType.equals("DROP_TABLE")){
                combinedSQLString = name + "##" + combinedSQLString;
                setSqlStatement(combinedSQLString, "masterSql");
            } else {
                String regionNode =tableRegionMap.get(name);
                setSqlStatement(name + "##" + combinedSQLString, "masterSql");
                setSqlStatement(combinedSQLString, regionNode);
            }

        }

        //当需要查询或者操作的表有多张，对应的逻辑。
        else {
            boolean inOneRegion = true;
            String firstRegion = tableRegionMap.get(tableList.get(0));
            for (String name : tableList) {
                String regionNode =tableRegionMap.get(name);
                if(!regionNode.equals(firstRegion)) {
                    inOneRegion = false;
                    break;
                }
            }
            if(inOneRegion) {
                String combinedSQLString = "client/" + clientName + "##" + operationType + "##" + sqlStatement;
                setSqlStatement(combinedSQLString, firstRegion);
            } else {
                StringBuilder sb = new StringBuilder();
                for (String tableName : tableList) {
                    sb.append(tableName).append(",");
                }
                if (sb.length() > 0) {
                    sb.deleteCharAt(sb.length() - 1);
                }
                sb.append("##").append("client/" + clientName).append("##").append(operationType).append("##").append(sqlStatement);
                //不同region下面的复杂查询
                setSqlStatement(sb.toString(), "masterSql");
            }
        }

        return "success";
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
     * 连接zookeeper集群
     */
    private void getConnect() throws IOException, InterruptedException, KeeperException {
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
        inputClientName();
    }

    /**
     * 监听自己创建的节点，以获得sql语句的执行结果
     * @param clientNode 自己的客户名
     */
    private void getExecuteSqlResult(String clientNode) throws InterruptedException, KeeperException {
        zkClient.getData("/master/client/" + clientNode, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                byte[] data;
                try {
                    data = zkClient.getData("/master/client/" + clientNode, this, null);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                String result = new String(data);
                if (result.startsWith("SELECT##")) {
                    result = result.substring(8);  // 去掉开头的 "SELECT##"
                    String[] rows = result.split("\n");
                    if (rows.length > 0) {
                        String[] columns = rows[0].split(", ");
                        int columnCount = columns.length;

                        // 打印表头
                        System.out.println("Query Result:");
                        for (String column : columns) {
                            String[] parts = column.split(": ");
                            System.out.printf("%-15s", parts[0]);
                        }
                        System.out.println();

                        // 打印每行数据
                        for (int i = 0; i < rows.length; i++) {
                            String[] values = rows[i].split(", ");
                            for (String value : values) {
                                String[] parts = value.split(": ");
                                System.out.printf("%-15s", parts[1]);
                            }
                            System.out.println();
                        }
                    }
                } else {
                    System.out.println(result);
                }

            }
        }, null);
    }

    /**
     * 当分布式数据库的表格有创建或者删除时，需要更新本地的tableRegionMap
     */
    private void retrieveMapFromZooKeeper() throws InterruptedException, KeeperException {

        byte[] data = zkClient.getData("/master", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                byte[] data;
                try {
                    data = zkClient.getData("/master", this, null);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                String storedData = new String(data);
                // 解析字符串并将键值对存储到Map中
                stringToMap(storedData);
//                System.out.println("Map retrieved from ZooKeeper successfully.");

            }
        }, null);
        String storedData = new String(data);
        stringToMap(storedData);
//        System.out.println("Map retrieved from ZooKeeper successfully.");
    }

    /**
     * 解析字符串并将键值对存储到Map中， 辅助实现retrieveMapFromZooKeeper()
     * @param storedData 传入的一定格式的字符串
     */
    private void stringToMap(String storedData) {
        tableRegionMap.clear();
        String[] keyValuePairs = storedData.split(";");
        for (String keyValuePair : keyValuePairs) {
            String[] parts = keyValuePair.split("->");
            if (parts.length == 2) {
                String key = parts[0];
                String value = parts[1];
                tableRegionMap.put(key, value);
            }
        }
    }
}