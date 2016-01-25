import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created by colinbiafore on 1/24/16.
 * Connects to twitter data stream and adds users
 * and tweets to neo4j
 */
public class TwitterStream {

    private static Authentication auth;
    private static JSONParser parser;
    private static TxHandler neoTx;
    private int amountAdded;

    /* Adds a Name, Timezone, and Tweet to neo4j */
    private void addToDB(String msg) {
        try {
            Object obj = parser.parse(msg);

            JSONObject jsonObject = (JSONObject)obj;
            JSONObject userObject = (JSONObject) jsonObject.get("user");

            JSONObject entityObject = (JSONObject) jsonObject.get("entities");
            JSONArray hashtagArray = (JSONArray)entityObject.get("hashtags");

            String tweet = jsonObject.get("text").toString();
            String userName = userObject.get("name").toString();
            String timeZone = userObject.get("time_zone").toString();


            /*
            System.out.println("Adding tweet: " + tweet);
            System.out.println("Name: " + userName);
            System.out.println("Timezone: " + timeZone);
            System.out.println("Hashtags:");


            if(hashtagArray.size() == 0) {
                System.out.println("None");
            } else {
                for(int i = 0; i < hashtagArray.size(); i++) {
                    System.out.println(hashtagArray.get(i).toString());
                }
            }
            */

            final String query = "CREATE (n:Person {name:'" + userName + "', time_zone:'" + timeZone + "', tweet:'" + tweet + "'}) RETURN n;";

            neoTx.send(query);
            amountAdded++;


        } catch(ParseException pe) {
            pe.printStackTrace();
        } catch(NullPointerException pe) {
            // Received something other than a created tweet (i.e. deletion)
        }
    }

    public TwitterStream() {

        /* OAuth information */
        String consumerKey = "jcaddGySJUnTzFeK0al8ha4Yl";
        String consumerSecret = "tYvNQxwHeLCotowIDs44O5bw4ODYVsm9sra8e8AsUz2seFLcrm";
        String token = "4615942873-hYv16zGpuOmSv0Rezk0eywtsDX6YjR4yr1TY7qA";
        String secret = "7L9lIgdEmd8bSK6Eh29oend1vMglGc75J9DcXEvpTvR1t";

        auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        parser = new JSONParser();
        neoTx = new TxHandler();
        amountAdded = 0;

    }

    public void fetch(String countString) {

        int count = Integer.parseInt(countString);

        /* Create an appropriately sized blocking queue */
        BlockingQueue<String> queue = new LinkedBlockingDeque<String>(count);

        /* Define endpoint. By default, delimited=length is set
         * (we need this for our processor) and stall warnings are on */
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);


        /* Create a new BasicClient. By default gzip is enabled. */
        BasicClient client = new ClientBuilder()
                .name("sampleExampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        /* Establish connection */
        client.connect();

        /* Fetch 'count' tweets and store them in neo4j */
        for(int msgRead = 0; msgRead < count; msgRead++) {
            if(client.isDone()) {
                System.out.println("Client connection closed unexpectedly: " +
                        client.getExitEvent().getMessage());
                break;
            }

            String msg = null;

            try {
                msg = queue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(msg == null) {
                System.out.println("Did not receive a message in 5 seconds");
            } else {
                addToDB(msg);
            }
        }

        client.stop();

        /* Print some statistics */
        System.out.println(amountAdded + " tweets added to neo4j");

        /* Reset the amount added */
        amountAdded = 0;

    }

}
