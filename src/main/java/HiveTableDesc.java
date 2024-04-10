import org.apache.hadoop.conf.Configuration;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class HiveTableDesc {
  public static void main(String[] args) {
    // 按需修改配置
    String url = "jdbc:hive2://172.26.1.28:10000/default";
    String user = "_SYSTEM";
    String password = "sys";
    String tableName = "user_test2";

//    Configuration conf = new Configuration();
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    try (Connection connection = DriverManager.getConnection(url, user, password)) {

      String sql = "DESC FORMATTED " + tableName;

      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        Map<String, String> tableInfo = new HashMap<>();

        while (resultSet.next()) {
          String columnName = resultSet.getString(1);
          String columnType = resultSet.getString(2);
          String comment = resultSet.getString(3);

          // 打印字段信息
          System.out.println("Column Name: " + columnName);
          System.out.println("Column Type: " + columnType);
          System.out.println("Comment: " + comment);
          System.out.println("----------------------");

          if (columnName.contains("Location")) {
            tableInfo.put("location", resultSet.getString(2).trim());
          } else if (columnName.contains("OutputFormat")) {
            String format = resultSet.getString(2).trim();
            tableInfo.put("format", format);
          } else if (columnName.contains("CreateTime")) {
            tableInfo.put("createTime", resultSet.getString(2).trim());
          } else if (columnType != null && columnType.contains("totalSize")) {
            tableInfo.put("totalSize", comment.trim());
          }

        }
        System.out.println("tableInfo:" + tableInfo);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
