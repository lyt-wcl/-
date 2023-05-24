package com.distributedDb.zk;

import org.apache.zookeeper.KeeperException;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库操作类，集成了一些常用的数据库操作
 */
public class DbOperations {

    /**
     * 执行show table table_name,返回对应的DDL语句
     * @param connection 数据库连接
     * @param showSql sql语句
     * @return 表格的DDL语句
     */
    public static String runShow(Connection connection, String showSql) {
        String result = "";
        try  {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(showSql);
            if (resultSet.next()) {
                String createStatement = resultSet.getString("Create Table");
                result = createStatement;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 执行select语句,将搜索结果序列化，返回字符串
     * @param connection 数据库连接
     * @param sqlStatement sql语句
     * @return 搜索结果序列化后的字符串
     */
    public static String runSelect(Connection connection, String sqlStatement){
        String result = "";
        try  {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlStatement);
            List<Map<String, Object>> queryResult = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    if(row.containsKey(columnName)) {
                        String tableName = metaData.getTableName(i);
                        columnName = tableName + "." + columnName;
                        row.put(columnName, columnValue);
                    } else {
                        row.put(columnName, columnValue);
                    }

                }
                queryResult.add(row);
            }
            StringBuilder sb = new StringBuilder("SELECT##");
            for (Map<String, Object> row : queryResult) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String columnName = entry.getKey();
                    Object columnValue = entry.getValue();
                    sb.append(columnName).append(": ").append(columnValue).append(", ");
                }
                sb.append("\n");
            }
            result = sb.toString();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 执行insert，delete，update，create，drop语句，返回执行结果字符串
     * @param connection 数据库连接
     * @param sqlStatement sql语句
     * @param operationType sql语句类型
     * @return 执行结果
     */
    public static String runUpdate(Connection connection, String sqlStatement, String operationType) throws UnsupportedEncodingException, InterruptedException, KeeperException {
        String result = "";
        try {
            PreparedStatement statement = connection.prepareStatement(sqlStatement);
            int rowsAffected = statement.executeUpdate();
            if(operationType.equals("CREATE_TABLE") || operationType.equals("DROP_TABLE")) {
                result = "execute success!";
            } else {

                if (rowsAffected > 0) {
                    result = "execute success!" + rowsAffected + " rows of table are affected.";
                } else {
                    result = "execute failed!";
                }
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 执行show tables，返回一个数据库的所有表名，返回字符串列表
     * @param connection 数据库连接
     * @return 表名的字符串列表
     */
    public static List<String> getTableNames(Connection connection) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            // 创建Statement对象
            statement = connection.createStatement();

            // 执行show tables查询
            String query = "SHOW TABLES";
            resultSet = statement.executeQuery(query);

            // 处理查询结果
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                tableNames.add(tableName);
            }
        } finally {
            // 关闭结果集和Statement对象
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
        return tableNames;
    }

    /**
     * 根据select结果往一个表插入数据
     * @param connection 数据库连接
     * @param tableName 表名
     * @param tableData select的结果，List的每一项代码select的一行结果
     */
    public static void insertTableData(Connection connection, String tableName, List<Map<String, Object>> tableData) throws SQLException{
        PreparedStatement statement = null;

        try {
            String insertQuery = "INSERT INTO " + tableName + " (";
            String valuesQuery = " VALUES (";

            for (String columnName : tableData.get(0).keySet()) {
                insertQuery += columnName + ",";
                valuesQuery += "?,";
            }
            insertQuery = insertQuery.substring(0, insertQuery.length() - 1) + ")";
            valuesQuery = valuesQuery.substring(0, valuesQuery.length() - 1) + ")";

            String insertStatement = insertQuery + valuesQuery;

            statement = connection.prepareStatement(insertStatement);

            for (Map<String, Object> rowData : tableData) {
                int parameterIndex = 1;
                for (Object value : rowData.values()) {
                    statement.setObject(parameterIndex, value);
                    parameterIndex++;
                }
                statement.executeUpdate();
            }
        } finally {
            // 关闭PreparedStatement对象
            if (statement != null) {
                statement.close();
            }
        }
    }
}
