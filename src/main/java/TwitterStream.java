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


import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * Created by colinbiafore on 1/24/16.
 * Connects to twitter data stream and adds users
 * and tweets to neo4j
 */
public class TwitterStream {

    private static Authentication auth;
    private static JSONParser parser;
    private static TxHandler neoTx;
    private ArrayList<Location> locationList;
    //private static Logger logger;


    public TwitterStream() {

        /* OAuth information */
        String consumerKey = "jcaddGySJUnTzFeK0al8ha4Yl";
        String consumerSecret = "tYvNQxwHeLCotowIDs44O5bw4ODYVsm9sra8e8AsUz2seFLcrm";
        String token = "4615942873-hYv16zGpuOmSv0Rezk0eywtsDX6YjR4yr1TY7qA";
        String secret = "7L9lIgdEmd8bSK6Eh29oend1vMglGc75J9DcXEvpTvR1t";

        auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        parser = new JSONParser();
        neoTx = new TxHandler();
        locationList = new ArrayList<Location>();

        /* logger will catch warnings and errors from twitter stream and print them */
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY,"ERROR");
        //logger = LoggerFactory.getLogger(com.twitter.hbc.httpclient.BasicClient.class);

        /* Limit twitter stream to tweets created in the US */
        Location.Coordinate bottomLeft = new Location.Coordinate(-124.7,25.3);
        Location.Coordinate topRight = new Location.Coordinate(-67.0,49.2);
        Location usaLocation = new Location(bottomLeft, topRight);
        locationList.add(usaLocation);

    }

    /* Connects to twitter through a Status Filter Endpoint */
    public void fetch(String countString) {


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
                    //System.out.println(msg);
                    addToNeo4j(msg);
                    //neoTx.query(query,true);
                }
            }
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }


        System.out.println("Done.");
        System.out.print("Closing connection...");
        client.stop();
        System.out.println("Done.");


    }

    /* Extracts hashtags from tweets and places them in a queryArray */
    private void addToNeo4j(String msg) {

        String locationQuery = "";
        String wordsQuery = "";

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(msg);

            /* latitude and longitude */
            JSONObject coordObject = (JSONObject) jsonObject.get("coordinates");
            JSONArray coordArray = (JSONArray) coordObject.get("coordinates");

            /* Remove apostraphe, backslash, and newline from tweets for now */
            String tweet = jsonObject.get("text").toString()
                    .replaceAll("'","")
                    .replaceAll("\"","")
                    .replaceAll("\n"," ");

            String[] tweetWordArray = tweet.split(" ");

            double longitude = (Double) coordArray.get(0);
            double latitude = (Double) coordArray.get(1);

            /* place = city name, fullPlace = city, state */
            JSONObject placeObject = (JSONObject) jsonObject.get("place");
            String place = placeObject.get("name").toString();
            String fullPlace = placeObject.get("full_name").toString();


            /* hashtags */
            JSONObject entitiesObject = (JSONObject) jsonObject.get("entities");
            JSONArray hashtagArray = (JSONArray) entitiesObject.get("hashtags");

            locationQuery = "CREATE (new:Location {title:'" + place +
                    "', text:'" + tweet +
                    "', full_name:'" + fullPlace +
                    "', latitude:" + latitude +
                    " , longitude:" + longitude + " }) " +
                    "WITH new " +
                    "MATCH (n:Location) " +
                    "WHERE n.latitude < new.latitude + 0.5 " +
                    "AND n.latitude > new.latitude - 0.5 " +
                    "AND n.longitude > new.longitude - 0.5 " +
                    "AND n.longitude < new.longitude + 0.5 " +
                    "AND n <> new " +
                    "WITH abs(n.latitude - new.latitude) AS x, abs(n.longitude - new.longitude) AS y, n, new " +
                    "CREATE (new)-[r1:CLOSE_TO {distance: sqrt((x*x) + (y*y))}]->(n)";

            /*

            CREATE (new:Location {title:"myPlace2", latitude: 40.9233, longitude: -73.155})
            WITH new
            MATCH (n:Location)
            WHERE n.latitude < new.latitude + 0.5
            AND n.latitude > new.latitude - 0.5
            AND n.longitude > new.longitude - 0.5
            AND n.longitude < new.longitude + 0.5
            AND n <> new
            WITH abs(n.latitude - new.latitude) AS x, abs(n.longitude - new.longitude) AS y, n, new
            CREATE (new)-[r1:CLOSE_TO {distance: sqrt((x*x) + (y*y))}]->(n)


             */



            /* MENTIONED_AT Relationship */
//            for(Object hashObj : hashtagArray) {
//                JSONObject hash = (JSONObject) hashObj;
//                String hashtag = hash.get("text").toString();
//                query += "CREATE (" + hashtag + ":Hashtag {hashtag:'" + hashtag +
//                        "'}) CREATE (" + hashtag + ")-[:MENTIONED_AT]->(new) ";
//
//                System.out.println(hashtag);
//
//            }


            /* Adding words to the database */
//            int wordNum = 0;
//            for(String word : tweetWordArray) {
//                wordsQuery += "CREATE (word" + wordNum + ":Word {word:'" + word +
//                        "', plcae:'" + place +
//                        "', full_name:'" + fullPlace +
//                        "', latitude:" + latitude +
//                        " , longitude:" + longitude + " }) ";
//
//                wordNum++;
//
//            }


            neoTx.query(locationQuery,false);
            //neoTx.query(wordsQuery,false);


        } catch(ParseException pe) {
            System.out.println("Parse exception error in convertToQuery.");
            pe.printStackTrace();
        } catch (NullPointerException npe) {
            System.out.println("Null Pointer Exception in convertToQuery.");
            System.out.println("Message: " + msg);
            npe.printStackTrace();
        }


    }

    /* Filter to make sure geographic data is available
     * and tweet came from the US
     *
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
