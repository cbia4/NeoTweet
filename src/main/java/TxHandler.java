import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;


/**
 * A class to manage interaction with the Neo4j server
 */

public class TxHandler {

    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    private static JSONParser parser;
    private String lastResponseMessage;
    private int lastResponseStatus;
    private Neo4jResponse lastResponse;
    private long currentID;


    /* TxHandler Constructor */
    public TxHandler() {
        parser = new JSONParser();
        lastResponseMessage = "";
        lastResponseStatus = 0;
        lastResponse = null;
        currentID = 0;

        send("MATCH (n:Location) RETURN max(n.location_id);");

    }

    /* gets max Location ID from neo4j and increments by 1 */
    private void setMaxID() {
        send("MATCH (n:Location) RETURN max(n.location_id);");
        if(lastResponse.didReceiveData) {
            JSONObject rowObject = (JSONObject) lastResponse.data.get(0);
            JSONArray idArray = (JSONArray) rowObject.get("row");
            if(idArray.get(0) == null) {
                currentID = 0;
            } else {
                currentID = (Long) idArray.get(0);
                currentID++;
            }
        }
    }

    /* Send a query to cypher */
    private void send(final String query) {

        /* Set the transaction URI */
        final String txUri = SERVER_ROOT_URI + "transaction/commit/";

        /* Establish connection with neo4j */
        WebResource resource = Client.create().resource(txUri);

        /* Put query into json format */
        String payload = "{\"statements\" : [ {\"statement\" : \"" + query + "\"} ]}";

        /* POST event */
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON)
                .entity( payload )
                .post( ClientResponse.class );

        /* Capture response status and json*/
        lastResponseStatus = response.getStatus();
        lastResponseMessage = response.getEntity(String.class);

        /* Print status or neo4j response */
        if(lastResponseStatus != 200) {
            System.out.println("Error. Status Code: " + lastResponseStatus);
            System.out.println("Response: " + lastResponseMessage);
        } else {
            parseResponse(lastResponseMessage);
            if(lastResponse.didReceiveError) {
                System.out.println("neo4j error: " + lastResponse.errorMessage);
            }
        }

        response.close();

    }

    /* Public method to send a query to neo4j */
    public void query(final String query) {
        send(query);
        if(lastResponse.didReceiveData) {
            System.out.println("neo4j response: " + lastResponse.data);
        }
    }

    // Returns true if new location added and false if location already exists
    private boolean addLocation(String text, double tweetID, double latitude, double longitude) {
        send("MATCH (nl:Location) WHERE nl.id = " + tweetID + " RETURN nl;");
        if(!lastResponse.didReceiveData) {
            send("CREATE (nl:Location {text: '" + text + "', id: " + tweetID + ", latitude: " + latitude + ", longitude: " + longitude + "});");
            return true;
        } else {
            return false;
        }
    }

    // Creates edges between locations that are close to each other (CLOSE_TO)
    private void relateLocations( double tweetID, double latitude, double longitude) {
        double bound = 0.25, latLowerBound = latitude - bound, latUpperBound = latitude + bound, longLowerBound = longitude - bound, longUpperBound = longitude + bound;
        send("MATCH (l1:Location) WHERE l1.id = " + tweetID +
                " WITH l1 MATCH (l2:Location) WHERE l2.latitude <= " + latUpperBound + " AND l2.latitude >= " + latLowerBound +
                " AND l2.longitude <= " + longUpperBound + " AND l2.longitude >= " + longLowerBound +
                " AND l1 <> l2 WITH l1, l2 CREATE (l1)-[:CLOSE_TO]->(l2);");
    }

    // Adds new user if not already present in the graph
    private void addUser(String username, int userID) {

        send("MATCH (newUser:User) WHERE newUser.id = " + userID + " RETURN newUser;");
        if(!lastResponse.didReceiveData)
            send("CREATE (newUser:User {username: '" + username + "', id: " + userID + "});");

    }

    // Creates edges between users and the locations they tweet at (TWEETED_AT)
    private void relateUserToLocation(int userID, double tweetID) {
        send("MATCH (u:User) WHERE u.id = " + userID + " WITH u MATCH (l:Location) WHERE l.id = " + tweetID +
                " WITH u,l CREATE (u)-[:TWEETED_AT]->(l);");
    }

    // TODO: Add frequency count
    private void addTopics(ArrayList<String> topics, int userID, double tweetID) {

        for(int i = 0; i < topics.size(); i++) {


            // check if topic has been mentioned by the user
            send("MATCH (u:User)-[:MENTIOINED]-(t:Topic) WHERE u.id = " + userID + " AND t.topic = '" + topics.get(i) +
                    "' RETURN t");

            // if the user has mentioned the topic before then simply create a new relationship between the topic and the new location
            if (lastResponse.didReceiveData) {
                send("MATCH (u:User)-[:MENTIOINED]-(t:Topic) WHERE u.id = " + userID + " AND t.topic = '" + topics.get(i) +
                        "' WITH t MATCH (l:Location) WHERE l.id = " + tweetID + " WITH t,l CREATE (t)-[:MENTIONED_AT]->(l);");
            }

            // if not then check whether the topic exists nearby
            else {

                // check whether the topic exists nearby
                send("MATCH (t:Topic)-[:MENTIONED_AT]-(l1:Location)-[:CLOSE_TO]-(l2:Location) WHERE t.topic = '" + topics.get(i) +
                        "' AND l2.id = " + tweetID + " RETURN t;");

                // if it does then create a new relationship between the (user -> topic) and (location -> topic)
                if (lastResponse.didReceiveData) {
                    send("MATCH (t:Topic)-[:MENTIONED_AT]-(l1:Location)-[:CLOSE_TO]-(l2:Location) WHERE t.topic = '" + topics.get(i) +
                            "' AND l2.id = " + tweetID + " WITH t, l2 CREATE UNIQUE (t)-[:MENTIONED_AT]->(l2);");

                    send("MATCH (t:Topic)-[:MENTIONED_AT]-(l:Location) WHERE t.topic = '" + topics.get(i) + "' AND l.id = " + tweetID +
                            " WITH t MATCH (u:User) WHERE u.id = " + userID + " WITH t,u CREATE UNIQUE (u)-[:MENTIONED]->(t)");
                }

                // if it doesn't then we need to create a new node and attach it to the location and the user
                else {

                    // create topic node and connect it to the location it was mentioned at
                    send("MATCH (l:Location) WHERE l.id = " + tweetID + " WITH l CREATE UNIQUE (t:Topic {topic: '" + topics.get(i) +
                            "'})-[:MENTIONED_AT]-(l);");

                    // connect the topic to the user who mentioned it
                    send("MATCH (u:User)-[:TWEETED_AT]-(l:Location)-[:MENTIONED_AT]-(t:Topic) WHERE u.id = " + userID +
                            " AND l.id = " + tweetID + " AND t.topic = '" + topics.get(i) + "' WITH u,t CREATE UNIQUE (u)-[:MENTIONED]->(t);");
                }

            }
        }
    }

    private void relateUsersByTopic(int userID) {
        send("MATCH (u1:User)-[:MENTIONED]-(t:Topic)-[:MENTIONED]-(u2:User) WHERE u1.id = " + userID +
                " WITH u1,u2 CREATE UNIQUE (u1)-[:TOPIC_CORRELATED]-(u2);");
    }

    private void relateTopicsByUser(int userID) {
        send("MATCH (t1:Topic)-[:MENTIONED]-(u1:User)-[:TOPIC_CORRELATED]-(u2:User)-[:MENTIONED]-(t2:Topic) WHERE u1.id = " + userID +
                " AND t1 <> t2 WITH t1, t2 CREATE UNIQUE (t1)-[:USER_CORRELATED]-(t2)");
    }



    // TODO: Organize data into neo4j
    public void addTweet(Tweet t) {

        // unmarshal arguments
        double tweetID = t.getTweetID();
        String username = t.getUsername();
        int userID = t.getUserID();
        String text = t.getText();
        text = text.replace("'","");
        double latitude = t.getLatitude();
        double longitude = t.getLongitude();
        ArrayList<String> topics = t.getTopics();

        // stop here if the tweet/location already exists
        if(!addLocation(text,tweetID,latitude,longitude))
            return;

        // create a CLOSE_TO relationship with nearby locations (.25 bounding box)
        relateLocations(tweetID,latitude,longitude);

        // adds a new user if user is not already present in database
        addUser(username,userID);

        // create a TWEETED_AT relationship between user and location
        relateUserToLocation(userID,tweetID);

        addTopics(topics,userID,tweetID);

        relateUsersByTopic(userID);

        relateTopicsByUser(userID);

    }

    /*
     * Our graph has Places, Locations, and Topics
     * Places are CLOSE_TO other Places
     * Locations are IN Places
     * Topics are MENTIONED_AT Locations
     */

    public void addTweet(String place, String fullPlace, double latitude, double longitude, String tweet, String[] topicArray) {

        setMaxID();

        double bound = 0.25;
        double latLowerBound = latitude - bound;
        double latUpperBound = latitude + bound;
        double longLowerBound = longitude - bound;
        double longUpperBound = longitude + bound;

        // Create a new place (city)

        // Create a new location (latitude, longitude, tweet)
        String newLocationNode = "CREATE (newLocation:Location {tweet: '" + tweet + "', latitude: " + latitude + ", longitude: " + longitude + ", location_id: " + currentID + "}) ";

        // Create a new relationship between the location and the place

        // First find out if a place nearby already exists, if it does then we don't need to create a new place and add to the one we found (place should be within .25 lat/long of us)
        // if it doesn't, then we look for places between .25 and .50 lat/long from us. Whatever is found we create a CLOSE_TO relationship with them
        // If nothing is found though, we need to create a new place

        String checkIfPlaceNearby = "MATCH (n:Place) WHERE n.latitude < " + latUpperBound +
                " AND n.latitude > " + latLowerBound +
                " AND n.longitude < " + longUpperBound +
                " AND n.longitude > " + longLowerBound;

        send(checkIfPlaceNearby + " RETURN n;");

        // if a nearby place exists then just create a relationship between the new location and existing place and we are done
        if(lastResponse.didReceiveData) {
            //System.out.println("Data returned: " + lastResponse.data);
            //System.out.println("Data set size returned: " + lastResponse.data.size());
            String createNewLocation = newLocationNode + "WITH newLocation " + checkIfPlaceNearby + " CREATE UNIQUE (newLocation)-[:IN]->(n);";
            //System.out.println(createNewLocation);
            send(createNewLocation);
        }

        // if not then create a new place, connect it to nearby places (.5) and create new location to connect to new place
        // in this case we can also assume there are no trending words to look out for nearby
        else {
            //System.out.println("No data returned. Create new place!");
            double extendedBound = 0.25;
            String searchForExistingPlace = "MATCH (n:Place) WHERE n.latitude < " + (latUpperBound + extendedBound) +
                    " AND n.latitude > " + (latLowerBound - extendedBound) +
                    " AND n.longitude < " + (longUpperBound + extendedBound) +
                    " AND n.longitude > " + (longLowerBound - extendedBound) +
                    " AND newPlace <> n ";


            String newPlaceNode = "CREATE (newPlace:Place {place: '" + place + "', full_place:'" + fullPlace + "', latitude: " + latitude + ", longitude: " + longitude + "}) ";
            String newPlaceLocationRelationship = "CREATE (newLocation)-[:IN]->(newPlace) ";
            String connectNewPlaceToExisting = "CREATE (newPlace)-[:CLOSE_TO]->(n) ";


            String createNewLocationAndPlace = newLocationNode +
                    newPlaceNode +
                    newPlaceLocationRelationship +
                    "WITH newPlace " +
                    searchForExistingPlace +
                    "WITH newPlace, n " +
                    connectNewPlaceToExisting;


            send(createNewLocationAndPlace);
        }


        String baseQuery;
        String matchQuery;
        String updateQuery;

        String locationMatchQuery = "MATCH (l:Location {location_id: " + currentID + "}) ";
        String newTopicNode;
        String newTopicRelationship;
        String newTopicQueries = locationMatchQuery;
        for (int i = 0; i < topicArray.length; i++) {
            baseQuery = "MATCH (n:Topic)--(l:Location) WHERE n.topic = '" + topicArray[i] +
                    "' AND l.latitude < " + latUpperBound +
                    " AND l.latitude > " + latLowerBound +
                    " AND l.longitude < " + longUpperBound +
                    " AND l.longitude > " + longLowerBound;

            matchQuery = baseQuery + " RETURN n;";
            send(matchQuery);

            // if a word was found within .25 of the newly created location
            if(lastResponse.didReceiveData) {
                updateQuery = baseQuery + " SET n.frequency = " + updateFrequency() + " WITH n MATCH (l2:Location {location_id: " + currentID + "}) CREATE UNIQUE (n)-[:MENTIONED_AT]->(l2);";
                send(updateQuery);
            } else {
                newTopicNode = "CREATE (newWord" + i + ":Topic {topic: '" + topicArray[i] + "', word_length: " + topicArray[i].length() + ", frequency: 1 }) ";
                newTopicRelationship = "CREATE (newWord" + i + ")-[:MENTIONED_AT]->(l) ";
                newTopicQueries += newTopicNode + newTopicRelationship;
            }
        }

        send(newTopicQueries);

    }

    private long updateFrequency() {
        JSONObject rowObject = (JSONObject) lastResponse.data.get(0);
        JSONArray rowArray = (JSONArray) rowObject.get("row");
        JSONObject nodeDataObject = (JSONObject) rowArray.get(0);
        return (Long) nodeDataObject.get("frequency") + 1;
    }

    /* Sends a query that creates a new tweet@Location and relates it to other tweets around it */
    public void createTweetAtLocation(String location, String fullLocation, String tweet, double latitude, double longitude) {
        String locationQuery = "CREATE (new:Location {location:'" + location +
                "', tweet:'" + tweet +
                "', full_location:'" + fullLocation +
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
                "CREATE (new)-[r:CLOSE_TO {distance: sqrt((x*x) + (y*y))}]->(n);";

        send(locationQuery);
    }

    /* Checks if a word is present near a location and either updates its frequency or creates a new node if the word is not present */
    public void updateWordFrequencyAtLocation(String[] wordArray, String location, String fullLocation, double latitude, double longitude) {
        double bound = 0.5;
        double latLowerBound = latitude - bound;
        double latUpperBound = latitude + bound;
        double longLowerBound = longitude - bound;
        double longUpperBound = longitude + bound;

        String baseQuery;
        String matchQuery;
        String updateQuery;

        for(String word : wordArray) {
            word = word.toLowerCase();

            baseQuery = "MATCH (n:Word) WHERE n.word = '" + word +
                    "' AND n.latitude < " + latUpperBound +
                    " AND n.latitude > " + latLowerBound +
                    " AND n.longitude < " + longUpperBound +
                    " AND n.longitude > " + longLowerBound;

            matchQuery = baseQuery + " RETURN n;";
            send(matchQuery);
            if(lastResponse.didReceiveData) {
                updateQuery = baseQuery + " SET n.frequency = n.frequency + 1;";
                send(updateQuery);
            } else {
                addWordAtLocation(word, location, fullLocation,latitude,longitude);
            }
        }
    }

    /* Called in updateWordFrequencyAtLocation when no word was matched at the location */
    private void addWordAtLocation(String word, String location, String fullLocation, double latitude, double longitude) {
        int wordLength = word.length();
        String query = "CREATE (new:Word {word:'" + word +
                "', latitude: " + latitude +
                ", longitude: " + longitude +
                ", location: '" + location +
                "', full_location: '" + fullLocation +
                "', word_length: " + wordLength +
                ", frequency: 1 } ) " +
                "WITH new " +
                "MATCH (n:Word) " +
                "WHERE n.latitude < new.latitude + 0.5 " +
                "AND n.latitude > new.latitude - 0.5 " +
                "AND n.longitude > new.longitude - 0.5 " +
                "AND n.longitude < new.longitude + 0.5 " +
                "AND n <> new " +
                "WITH abs(n.latitude - new.latitude) AS x, abs(n.longitude - new.longitude) AS y, n, new " +
                "CREATE (new)-[r:CLOSE_TO {distance: sqrt((x*x) + (y*y))}]->(n) " +
                "WITH new " +
                "MATCH (m:Word) " +
                "WHERE new.word = m.word AND new <> m " +
                "CREATE UNIQUE (new)-[r:SAME]->(m);";

        send(query);

    }

    /* Parses neo4j response
     * Returns response object if successful
     * null if unsuccessful
     */
    private void parseResponse(String responseMessage) {

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(responseMessage);
            JSONArray errors = (JSONArray) jsonObject.get("errors");
            JSONArray results = (JSONArray) jsonObject.get("results");
            lastResponse = new Neo4jResponse(results,errors);
        } catch(ParseException pe) {
            pe.printStackTrace();
        }
    }

    /* Class to hold JSON response data from neo4j */
    private class Neo4jResponse {

        private JSONArray data;

        public boolean didReceiveError;
        public boolean didReceiveData;

        public String errorMessage;

        public Neo4jResponse(JSONArray results, JSONArray errors) {
            if(!errors.isEmpty()) {
                JSONObject messageObject = (JSONObject) errors.get(0);
                errorMessage = messageObject.get("message").toString();
                didReceiveError = true;
                didReceiveData = false;
                data = null;
            } else {
                errorMessage = "No error in response";
                didReceiveError = false;
                JSONObject resultsObject = (JSONObject) results.get(0);
                data = (JSONArray) resultsObject.get("data");
                didReceiveData = !data.isEmpty();
            }
        }

        public JSONArray getData() { return data; }

    }


}
