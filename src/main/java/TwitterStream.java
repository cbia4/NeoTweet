import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;
import org.slf4j.impl.SimpleLogger;


import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * Created by colinbiafore on 1/24/16.
 * Connects to twitter data stream and adds users
 * and tweets to neo4j
 */

class TwitterStream {

    private static Authentication auth;
    private static JSONParser parser;
    private static TxHandler neoTx;
    private ArrayList<Location> locationList;
    //private static Logger logger;


    TwitterStream() {

        String[] oauth = new String[4];
        File configFile = new File("/Users/colinbiafore/Desktop/research/db_resources/oauth.txt");
        if(!configFile.exists()) {
            System.err.println("Error: Oauth file not found. Exiting");
            System.exit(1);
        }

        try {
            String line;
            int i = 0;
            FileInputStream fis = new FileInputStream(configFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            while((line = br.readLine()) != null) {
                oauth[i] = line;
                i++;
            }
            br.close();
            fis.close();
        } catch(IOException e) {
            System.err.println("Error: Failed to read oauth file. Exiting");
            e.printStackTrace();
            System.exit(1);
        }

        auth = new OAuth1(oauth[0], oauth[1], oauth[2], oauth[3]);
        parser = new JSONParser();
        neoTx = new TxHandler();
        locationList = new ArrayList<Location>();

        /* logger will catch warnings and errors from twitter stream and print them */
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY,"ERROR");

        /* Limit twitter stream to tweets created in the US */
        Location.Coordinate bottomLeft = new Location.Coordinate(-124.7,25.3);
        Location.Coordinate topRight = new Location.Coordinate(-67.0,49.2);
        Location usaLocation = new Location(bottomLeft, topRight);
        locationList.add(usaLocation);

    }

    /* Connects to twitter through a Status Filter Endpoint */
    void fetch(String countString) {


        int count = Integer.parseInt(countString);

        /* Create an appropriately sized blocking queue */
        BlockingQueue<String> queue = new LinkedBlockingDeque<String>(count);

        /* Create an endpoint that gets tweets within the US (POST) */
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.locations(locationList);

        /* Create a client to connect with Twitter */
        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        /* Establish Connections*/
        System.out.print("Connecting to twitter... ");
        client.connect();
        System.out.println("Done.");

        /* Add queued messages to neo4j */
        System.out.println("Fetching data... ");
        try {
            for (int msgRead = 0; msgRead < count; msgRead++) {
                String msg = queue.take();
                if(isTweetWithGeo(msg)) {

                    /* Parse for relevant information and add to neo4j */
                    updateNeo4j(msg);

                }
            }
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

        /* Close connection to twitter */
        System.out.println("Done.");
        System.out.print("Closing connection...");
        client.stop();
        System.out.println("Done.");


    }

    /* Extracts location and words from a tweet (JSON) and update neo4j accordingly */
    private void updateNeo4j(String msg) {

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(msg);

            /* Parse JSON for the fields we are interested in */
            JSONObject coordObject = (JSONObject) jsonObject.get("coordinates");
            JSONArray coordArray = (JSONArray) coordObject.get("coordinates");
            JSONObject placeObject = (JSONObject) jsonObject.get("place");

            /* Parse JSON for tweet */
            /*Remove apostraphe, backslash, newline, comma, and colon from tweets for now */
            String tweet = jsonObject.get("text").toString()
                    .replaceAll("'","")
                    .replaceAll("\"","")
                    .replaceAll("\n"," ")
                    .replaceAll(",","")
                    .replaceAll(":","");

            /* Put all words of tweet in an array */
            String[] topicArray = tweet.split(" ");

            /* Parse JSON for Location attributes */
            String location = placeObject.get("name").toString();
            String fullLocation = placeObject.get("full_name").toString();
            double longitude = (Double) coordArray.get(0);
            double latitude = (Double) coordArray.get(1);

            //neoTx.createTweetAtLocation(location, fullLocation, tweet, latitude, longitude);
            //neoTx.updateWordFrequencyAtLocation(topicArray, location, fullLocation, latitude, longitude);
            System.out.println("Adding tweet: " + tweet);
            neoTx.addTweet(location, fullLocation, latitude, longitude, tweet, topicArray);

        } catch(ParseException pe) {
            System.out.println("Parse exception error in updateNeo4j.");
            pe.printStackTrace();
        } catch (NullPointerException npe) {
            System.out.println("Null Pointer Exception in updateNeo4j.");
            System.out.println("Message: " + msg);
            npe.printStackTrace();
        }
    }

    /* Filter to make sure geographic data is available
     * and tweet came from the US
     */
    private boolean isTweetWithGeo(String msg) {
        try {
            JSONObject jsonObject = (JSONObject) parser.parse(msg);
            JSONObject coordObject = (JSONObject) jsonObject.get("coordinates");
            JSONObject placeObject = (JSONObject) jsonObject.get("place");
            boolean isTweet = jsonObject.containsKey("text");
            if(isTweet && coordObject != null && placeObject != null) {
                /* Make sure tweet was created in the US */
                if(placeObject.get("country_code").toString().equals("US")) {
                    return true;
                }
            }
        } catch(ParseException pe) {
            pe.printStackTrace();
        }
        return false;
    }

}
