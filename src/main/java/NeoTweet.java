/**
 * Created by colinbiafore on 1/24/16.
 * Creates a Neo4J Client
 * Client is able to interact with Neo4J and fetch tweets from Twitter
 */

import java.util.*;
import java.io.*;

public class NeoTweet {

    public static void printInstructions() {
        System.out.println("Options:");
        System.out.println("1) Fetch Tweets from Twitter's Public Data Stream");
        System.out.println("2) Transfer tweets from MySQL to neo4j");
        System.out.println("3) Exit");
        System.out.println();
    }

    public static void main(String[] args) {

        SQLConnect sqlConnect = null;
        String jdbcUrl, username, password;
        // Read configuration file
        File configFile = new File("/Users/colinbiafore/Desktop/research/db_resources/config.txt");
        if(!configFile.exists()) {
            System.err.println("Error: Configuration file not found. Exiting");
            System.exit(1);
        }

        try {
            FileInputStream fis = new FileInputStream(configFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            jdbcUrl = br.readLine();
            username = br.readLine();
            password = br.readLine();
            sqlConnect = new SQLConnect(jdbcUrl, username, password);
            br.close();
            fis.close();
        } catch(IOException e) {
            System.err.println("Error: Failed to read configuration file. Exiting");
            e.printStackTrace();
            System.exit(1);
        }


        Scanner sc = new Scanner(System.in);
        boolean inSession = true;
        int input;
        System.out.println("==========NEO-TWEET==========");

        while(inSession) {
            printInstructions();
            System.out.print("-> ");
            input = sc.nextInt();
            if(input == 1) {
                NeoClient client = new NeoClient();
                client.printInstructions();
                client.getInput();
            } else if(input == 2) {
                sqlConnect.connect();
                sqlConnect.transferData();
            } else if(input == 3) {
                inSession = false;
            }
        }

        System.out.println("Goodbye.");
    }

}
