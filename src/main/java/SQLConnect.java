/**
 * Created by colinbiafore on 5/24/16.
 */

import java.sql.*;
import java.util.ArrayList;
import java.io.*;

public class SQLConnect {

    private String url;
    private String username;
    private String password;
    private TxHandler tx;
    private Connection conn;
    private String[] tableNames;
    private static final int NUM_TABLES = 1231;

    // Constructor
    public SQLConnect(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.tx = new TxHandler();
        this.conn = null;
        this.tx = new TxHandler();
        this.tableNames = new String[NUM_TABLES];
    }

    // Load JDBC driver and connect to MySQL database
    public void connect() {

        // Attempt to load the JDBC driver
        System.out.println("Loading driver...");
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver loaded!");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot find driver in the class path!", e);
        }

        System.out.println("Connecting database...");

        // Attempt to connect to MySQL server
        try {
            conn = DriverManager.getConnection(url, username, password);
            System.out.println("Database connected!");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect to the database!", e);
        }
    }

    // Transfer tweets from MySQL to neo4j
    public void transferData() {
        getTablesFromFile();
        for(int i = 0; i < NUM_TABLES; i++) {
            System.out.println("Loading " + tableNames[i]);
            extractData(tableNames[i]);
            System.out.println("Done");
        }
    }

    // loads table names from db_file.txt
    private void getTablesFromFile() {
        File tableFile = new File("/Users/colinbiafore/Desktop/research/db_resources/db_file.txt");
        if(!tableFile.exists()) {
            System.err.println("Error: Table file not found. Exiting");
            System.exit(1);
        }

        try {
            int i = 0;
            FileInputStream fis = new FileInputStream(tableFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            String[] splitInput;
            while((line = br.readLine()) != null) {
                splitInput = line.split(" ");
                tableNames[i] = splitInput[0];
                i++;
            }

            br.close();
            fis.close();

        } catch(IOException e) {
            System.err.println("Error: Failed to read table file. Exiting");
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Queries r-shief dataset and adds returned data with a neo4j txHandler
    private void extractData(String tableName) {
        Statement st;
        ResultSet rs;
        String sqlQuery;
        Tweet t;
        try {
            sqlQuery = "select T.twitter_id, T.text, T.from_user, T.from_user_id, T.geo " +
                    "from " + tableName + " T where T.geo != 'N;' and T.language = 'en'";
            st = conn.createStatement();
            rs = st.executeQuery(sqlQuery);
            while(rs.next()) {
                t = new Tweet(
                        rs.getDouble("twitter_id"),
                        rs.getString("from_user"),
                        rs.getInt("from_user_id"),
                        rs.getString("text"),
                        getCoordinates(rs.getString("geo")),
                        getTopics(rs.getString("text")));

                // add to neo4j with txHandler here
                tx.addTweet(t);
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }

    }

    // parses geo for coordinates and places them in an ArrayList
    private ArrayList<Double> getCoordinates(String s) {
        ArrayList<Double> coordinates = new ArrayList<Double>();
        String[] tokens = s.split("d:");
        String[] latSplit = tokens[1].split(";");
        String [] longSplit = tokens[2].split(";");
        coordinates.add(Double.parseDouble(latSplit[0]));
        coordinates.add(Double.parseDouble(longSplit[0]));
        return coordinates;
    }

    // parses text for hashtags and places them in an ArrayList
    private ArrayList<String> getTopics(String s) {
        ArrayList<String> topics = new ArrayList<String>();
        String[] words = s.split(" ");
        for(String word : words) {
            if(word.length() > 0 && word.charAt(0) == '#') {
                topics.add(word.substring(1));
            }
        }

        return topics;
    }

}
