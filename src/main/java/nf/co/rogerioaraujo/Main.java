package nf.co.rogerioaraujo;

import nf.co.rogerioaraujo.dao.Database;
import nf.co.rogerioaraujo.service.TruncateService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {

    private static Long tempoInicial;

    private static final Integer MAX = 100;

    private static final String sqlInserir = "INSERT INTO Usuario(id, Nome) VALUES (?,?);";
    private static final String sqlAtualizar = "UPDATE Usuario SET updated = TRUE WHERE id = ?";
    private static final String sqlDeletar = "UPDATE Usuario SET deleted = TRUE WHERE id = ?";
    private static final String sqlConfigurarId = "INSERT INTO Controle(indexNome, indexValue) VALUES ('default', 0);";
    private static final String sqlRecuperarId = "UPDATE Controle SET indexValue = (indexValue + 1) WHERE indexNome = 'default' RETURNING indexValue;";

    private static final BlockingQueue<Integer> queueAtualizar = new ArrayBlockingQueue<>(3);
    private static final BlockingQueue<Integer> queueDeletar = new ArrayBlockingQueue<>( 3);

    private static Thread restaurarAtualizar = null;
    private static Thread restaurarDeletar = null;

    private static Boolean continuar = true;

    public static void main(String[] args) throws Exception {

        Connection conn = Database.getConnection();
        TruncateService truncate = new TruncateService();
        truncate.truncade();

        try {
            conn.prepareStatement(sqlConfigurarId).executeUpdate();
        } catch (Exception ex) {
            // do nothing
        }

        Runnable inserir = () -> {
            try {

                ResultSet rs = conn.prepareStatement(sqlRecuperarId).executeQuery();

                int localId = 0;
                if (rs.next()) localId = rs.getInt("indexValue");
                rs.close();

                if (localId > MAX) parar();
                else {

                    PreparedStatement stmtInserir = conn.prepareStatement(sqlInserir);

                    stmtInserir.setInt(1, localId);
                    stmtInserir.setString(2, String.format("Nome - %d", localId));
                    stmtInserir.executeUpdate();

                    queueAtualizar.put(localId);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        };

        Runnable atualizar = () -> {
            try {
                Integer localId = Integer.valueOf(queueAtualizar.take());
                PreparedStatement stmtAtualizar = conn.prepareStatement(sqlAtualizar);

                stmtAtualizar.setInt(1, localId);
                stmtAtualizar.executeUpdate();

                queueAtualizar.put(localId);
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        };

        Runnable deletar = () -> {
            try {
                Integer localId = Integer.valueOf(queueDeletar.take());
                PreparedStatement stmtDeletar = conn.prepareStatement(sqlDeletar);

                stmtDeletar.setInt(1, localId);
                stmtDeletar.executeUpdate();

                if (localId.equals(MAX)) parar();
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        };

        restaurarAtualizar = new Thread(() -> {
            try {
                Connection localConn = Database.getConnection();

                String sqlRestaurarAtualiza = "SELECT id FROM Usuario WHERE updated = FALSE;";

                ResultSet rs = localConn.prepareStatement(sqlRestaurarAtualiza).executeQuery();

                while (rs.next()) queueAtualizar.put(rs.getInt("id"));

            } catch (SQLException | InterruptedException ex) {
                ex.printStackTrace();
            }
        });

        restaurarDeletar = new Thread(() -> {
            try {
                Connection localConn = Database.getConnection();

                String sqlRestaurarAtualiza = "SELECT id FROM Usuario WHERE deleted = FALSE;";

                ResultSet rs = localConn.prepareStatement(sqlRestaurarAtualiza).executeQuery();

                while (rs.next()) queueDeletar.put(rs.getInt("id"));

            } catch (SQLException | InterruptedException ex) {
                ex.printStackTrace();
            }
        });

        tempoInicial
                = System.currentTimeMillis();

        restaurarAtualizar.start();
        restaurarDeletar.start();

        while (getContinuar()) {

            new Thread(inserir).start();
            new Thread(atualizar).start();
            new Thread(deletar).start();
        }


    }

    public static void parar() {
        if(restaurarAtualizar.isAlive() || restaurarDeletar.isAlive()) return;
        else {
            Long tempoFinal = System.currentTimeMillis();
            System.out.printf("\n\n" + (tempoFinal - tempoInicial) + "ms\n\n");
            setContinuar(false);
        }
    }

    public static Boolean getContinuar() {
        return continuar;
    }

    public static void setContinuar(Boolean continuar) {
        Main.continuar = continuar;
    }
}