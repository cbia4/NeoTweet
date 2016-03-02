import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.ws.rs.core.MediaType;


/**
 * A class to manage interaction with the Neo4j server
 */

public class TxHandler {

    final private String SERVER_ROOT_URI;
    private static JSONParser parser;
    private String lastResponseMessage;
    private int lastResponseStatus;
    private Neo4jResponse lastResponse;

    /* TxHandler Constructor */
    public TxHandler() {

        this.SERVER_ROOT_URI = "http://localhost:7474/db/data/";
        parser = new JSONParser();
        lastResponseMessage = "";
        lastResponseStatus = 0;
        lastResponse = null;
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
            lastResponse = parseResponse(lastResponseMessage);
        }

        response.close();

    }

    /* Public method to send a query to neo4j */
    public void query(final String query) {
        send(query);

    }

    /* Sends a query that creates a new tweet@Location and relates it to other tweets around it */
    public void createTweetAtLocation(String location, String fullLocation, String tweet, double latitude, double longitude) {
        String locationQuery = "CREATE (new:Location {title:'" + location +
                "', text:'" + tweet +
                "', full_name:'" + fullLocation +
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
                updateQuery = " SET n.frequency = n.frequency + 1;";
                send(updateQuery);
            } else {
                addWordAtLocation(word, location, fullLocation,latitude,longitude);
            }
        }
    }

    /* Called in updateWordFrequencyAtLocation when no word was matched at the location */
    private void addWordAtLocation(String word, String location, String fullLocation, double latitude, double longitude) {
        int wordLength = word.length();
        String query = "Create (new:Word {word:'" + word +
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
                "CREATE (new)-[r1:CLOSE_TO {distance: sqrt((x*x) + (y*y))}]->(n)";

        send(query);

    }

    /* Parses neo4j response
     * Returns response object if successful
     * null if unsuccessful
     */
    private Neo4jResponse parseResponse(String responseMessage) {

        Neo4jResponse response = null;

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(responseMessage);
            JSONArray errors = (JSONArray) jsonObject.get("errors");
            JSONArray results = (JSONArray) jsonObject.get("results");
            response = new Neo4jResponse(results,errors);
        } catch(ParseException pe) {
            pe.printStackTrace();
        }

        return response;
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

    }


}
