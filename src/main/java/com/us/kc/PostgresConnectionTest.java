package com.us.kc;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

//javac PostgresConnectionTest.java
// (Use ; instead of : on Windows below.)
//java -cp .:postgresql-42.7.3.jar PostgresConnectionTest
public class PostgresConnectionTest {

    public static void main(String[] args) {
        // <DB_HOST> can be external IP or service name
        String url = "jdbc:postgresql://<DB_HOST>:5432/keycloak"; 
        String user = "keycloakuser";
        String password = "Pass";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            System.out.println(" Connected to PostgreSQL successfully!");

            // Run a test query
            ResultSet rs = stmt.executeQuery("SELECT version();");
            if (rs.next()) {
                System.out.println("Postgres version: " + rs.getString(1));
            }

        } catch (Exception e) {
            System.err.println(" Connection failed!");
            e.printStackTrace();
        }
    }
}
