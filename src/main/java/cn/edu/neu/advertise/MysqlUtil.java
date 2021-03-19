package cn.edu.neu.advertise;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author 32098
 */
public class MysqlUtil {
    public static Connection getConnection(){
        String url = "jdbc:mysql://master:3306/user_advertise?useUnicode=true&characterEncoding=utf-8";
        String user = "root";
        String password = "Hive@2020";
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }
}
