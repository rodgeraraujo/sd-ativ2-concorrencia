package nf.co.rogerioaraujo.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {

    private static final String url = "jdbc:postgresql://localhost:5432/ativ1";
    private static final String user = "postgres";
    private static final String password = "docker";

    public static Connection getConnection  () throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
