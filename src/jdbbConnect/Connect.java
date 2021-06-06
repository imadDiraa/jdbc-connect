package jdbbConnect;

import java.sql.*;
import java.io.*;
import java.util.*;
import java.sql.Statement;


public class Connect  {
    // Nome driver JDBC e URL DB
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/?user=root";

    //  Credenziali DB
    static final String USER = "your_user";
    static final String PASS = "your_password";

    private static void load_driver() throws ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
    }



    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // loading the driver
        System.out.println("loading driver...");
        load_driver();

        // get db connection
        System.out.println("connecting to db...");
        Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
        System.out.println("connected to db, nice job");

        // creating db and table if they are not exists
        System.out.println("Creazione database and table if not exists");
        Statement statement = connection.createStatement();
        String query = "CREATE DATABASE if not exists DB_Comuni";
        statement.executeUpdate(query);
        query = "CREATE TABLE IF NOT EXISTS DB_Comuni.Comuni" +
                "(id MEDIUMINT NOT NULL AUTO_INCREMENT," +
                "name VARCHAR(255) NOT NULL," +
                "slug VARCHAR(255) NOT NULL," +
                "lat DECIMAL(10 , 2 ) NOT NULL," +
                "lng DECIMAL(10 , 2 ) NOT NULL," +
                "codice_provincia_istat INTEGER NOT NULL," +
                "codice_comune_istat INTEGER NOT NULL," +
                "codice_alfanumerico_istat INTEGER NOT NULL," +
                "capoluogo_provincia BOOL NOT NULL," +
                "capoluogo_regione BOOL NOT NULL," +
                "PRIMARY KEY (id))";;
        statement.executeUpdate(query);

        // data importing
        System.out.println("data importing...");
        String sql3 = "insert into Comuni "
                + "(name,"
                + "slug,"
                + "lat,"
                + "lng,"
                + "codice_provincia_istat,"
                + "codice_comune_istat,"
                + "codice_alfanumerico_istat,"
                + "capoluogo_provincia,"
                + "capoluogo_regione) "
                + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement insert = connection.prepareStatement(sql3);
        data_importing(insert);
        System.out.println("data imported successfully");


        // close connection and statement
        statement.close();
        connection.close();
    }

    public static void data_importing(PreparedStatement s) {
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File("C:\\Users\\imadd\\ideaProjects\\jdbc-db-connect\\src\\jdbbConnect\\comuni.csv"));
            scanner.nextLine();
            while(scanner.hasNextLine())
            {
                String [] split = scanner.nextLine().split(";");
                s.setString(1, split[1]);
                s.setString(2, split[2]);
                s.setFloat(3, Float.parseFloat(split[3]));
                s.setFloat(4, Float.parseFloat(split[4]));
                s.setInt(5, Integer.parseInt(split[5]));
                s.setInt(6, Integer.parseInt(split[6]));
                s.setInt(7, Integer.parseInt(split[7]));
                s.setInt(8, Integer.parseInt(split[8]));
                s.setInt(9, Integer.parseInt(split[9]));

                s.execute();
            }
        }
        catch (FileNotFoundException | SQLException fe)
        {
            fe.printStackTrace();
        }
        finally
        {
            scanner.close();
        }
    }
}

