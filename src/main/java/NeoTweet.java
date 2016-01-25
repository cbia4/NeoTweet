/**
 * Created by colinbiafore on 1/24/16.
 * Creates a Neo4J Client
 * Client is able to interact with Neo4J and fetch tweets from Twitter
 */
public class NeoTweet {
    public static void main(String[] args) {
        System.out.println("==========NEO-TWEET==========");

        NeoClient client = new NeoClient();
        client.getInput();


    }

}
