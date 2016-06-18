import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

/**
 * Created by colinbiafore on 6/1/16.
 * A class that allows manual entry of twitter data to neo4j
 */

class TweetTester {


    private TxHandler tx;
    private Scanner sc;

    TweetTester() {
        tx = new TxHandler();
        sc = new Scanner(System.in);
    }

    private Tweet createNewTweet() {

        sc.nextLine();
        System.out.print("Enter username: ");
        String username = sc.nextLine();
        System.out.print("Enter userID: ");
        int userID = sc.nextInt();
        sc.nextLine();
        System.out.print("Enter tweet: ");
        String text = sc.nextLine();
        System.out.print("Enter tweetID: ");
        double tweetID = sc.nextDouble();
        sc.nextLine();
        System.out.print("Enter latitude: ");
        double latitude = sc.nextDouble();
        sc.nextLine();
        System.out.print("Enter longitude: ");
        double longitude = sc.nextDouble();
        sc.nextLine();
        ArrayList<Double> coordinates = new ArrayList<Double>();
        coordinates.add(latitude);
        coordinates.add(longitude);
        System.out.print("Enter topics: ");
        String topicString = sc.nextLine();
        String[] topicList = topicString.split(" ");
        ArrayList<String> topics = new ArrayList<String>();
        Collections.addAll(topics, topicList);


        System.out.print("Confirm adding tweet? (1=yes, 2=no): ");
        int opt = sc.nextInt();
        if(opt == 1) {
            return new Tweet(tweetID, username, userID, text, coordinates, topics);
        } else {
            return null;
        }

    }

    private void printInstructions() {
        System.out.println("-------------");
        System.out.println("Welcome to the Neo-Tweet Testing Center");
        System.out.println("This module is designed to help implement the neo4j tweet graph");
        System.out.println("Each entry correlates to one tweet that will be added to neo4j");
        System.out.println("1) Add a test tweet to neo4j.");
        System.out.println("2) Exit the tweet tester");
    }


    void run() {

        boolean inSession = true;
        Tweet t;
        int opt;

        while(inSession) {

            printInstructions();
            System.out.print("-> ");
            opt = sc.nextInt();

            if(opt == 1) {
                t = createNewTweet();
                if(t != null)
                    tx.addTweet(t);
            } else if(opt == 2) {
                inSession = false;
            } else {
                System.out.println("Invalid option.");
            }
        }
    }
}
