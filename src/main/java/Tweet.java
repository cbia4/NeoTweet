import java.util.ArrayList;

/**
 * Created by colinbiafore on 5/28/16.
 * A class to store relevant tweet data
 */

class Tweet {

    private double tweetID;
    private String username;
    private int userID;
    private String text;
    private double latitude;
    private double longitude;
    private ArrayList<String> topics;

    Tweet(double tweetID, String username, int userID, String text,
                 ArrayList<Double> coordinates, ArrayList<String> topics) {

        this.tweetID = tweetID;
        this.username = username;
        this.userID = userID;
        this.text = text;
        this.latitude = coordinates.get(0);
        this.longitude = coordinates.get(1);
        this.topics = topics;

    }

    double getTweetID() { return tweetID; }
    String getUsername() { return username; }
    int getUserID() { return userID; }
    String getText() { return text; }
    double getLatitude() { return latitude; }
    double getLongitude() { return longitude; }
    ArrayList<String> getTopics() { return topics; }

    public void printData() {
        System.out.println("--------------------------------------");
        System.out.println("Tweet ID: " + tweetID + "\n" + "Username: " + username + "\n" +
        "User ID: " + userID + "\n" + "Text: " + text + "\n" + "Geo: " + latitude + ", " +
        longitude + "\n" + "Topics: ");
        for(String topic: topics) {
            System.out.println(topic);
        }
    }

}
