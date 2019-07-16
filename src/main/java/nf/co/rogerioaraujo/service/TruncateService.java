package nf.co.rogerioaraujo.service;

import nf.co.rogerioaraujo.dao.Database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TruncateService {
    private Connection connection;
    private Database db = new Database();

    public TruncateService() throws SQLException {
        this.connection = db.getConnection();
    }

    public void truncade() {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("TRUNCATE usuario;");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
