import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class HiveTableDesc {
  public static void main(String[] args) {
    // 按需修改配置
    // 使用用户名、密码
    String url = "jdbc:hive2://localhost:10000/default"; // 本机使用docker搭建Hive：https://github.com/big-data-europe/docker-hive
    String user = "";
    String password = "";
    String tableName = "pokes";

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
            if (comment != null) {
              tableInfo.put("totalSize", comment.trim());
            }
          }

        }
        System.out.println("tableInfo:" + tableInfo);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
