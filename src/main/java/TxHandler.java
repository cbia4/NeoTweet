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

    /* TxHandler Constructor */
    public TxHandler() {

        this.SERVER_ROOT_URI = "http://localhost:7474/db/data/";
        parser = new JSONParser();

    }

    // TODO: Parse a response carrying twitter data
    /* Parses neo4j response */
    private void parseResponse(String neoResponse) {

        try {

            Object obj = parser.parse(neoResponse);
            JSONObject jsonObject = (JSONObject)obj;

            JSONArray results = (JSONArray)jsonObject.get("results");
            JSONObject data = (JSONObject)results.get(0);
            JSONArray dataArray = (JSONArray)data.get("data");

            for(int i = 0; i < dataArray.size(); i++) {
                JSONObject row = (JSONObject)dataArray.get(i);
                JSONArray dataInRow = (JSONArray)row.get("row");
                JSONObject info = (JSONObject)dataInRow.get(0);
                String tweet = info.get("tweet").toString();
                //String userAge = info.get("age").toString();
                //int age = Integer.parseInt(userAge);
                System.out.println("Tweet: " + tweet);
                //System.out.println(" Age: " + age);
                System.out.println("------------------------");
            }
        } catch(ParseException pe) {
            pe.printStackTrace();
        }
    }

    /* Send a query to cypher */
    private void send(final String query, boolean shouldRespond) {

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
        int statusCode = response.getStatus();
        String neoResponse = response.getEntity(String.class);

        /* Print status or neo4j response */

        if(shouldRespond) {
            if(statusCode != 200) {
                System.out.println("Error. Status: " + statusCode);
            } else {
                System.out.println("Success.");
                System.out.println("Response: " + neoResponse);
            }

            System.out.println();
        }

        response.close();

    }

    // TODO: Update client input options

    /* Send a query in cypher syntax to neo4j */
    public void query(final String query, boolean shouldRespond) {
        send(query, shouldRespond);
    }

    /* Add a name and an age */
    public void add(final String userName, final String tweet, final String place,
                    final double latitude, final double longitude ) {
        String query = "CREATE (n:Tweet {name:'" + userName +
                "', place:'" + place +
                "', tweet:'" + tweet +
                "', latitude:" + latitude +
                ", longitude:" + longitude +
                "}) RETURN n;";

        send(query,true);
    }

    /* Connect one node to another by name */
    public void addRel(final String name1, final String relType, final String name2) {

        final String query = "MATCH (a:Person),(b:Person) " +
                "WHERE a.name = '" + name1 + "' AND b.name = '" + name2 + "' " +
                "CREATE (a)-[r:" + relType + " {name: a.name + '-[" + relType + "]->' + b.name}]->(b) " +
                "RETURN r;";

        send(query,true);
    }

    /* Delete a node matching the specified name */
    public void delete(final String name) {
        final String query = "MATCH (n:Person {name:'" + name + "'}) DETACH DELETE n;";
        send(query,true);
    }

    /* Update an nodes name and age */
    public void update(final String oldName, final String newName, final String newAge) {
        int age = Integer.parseInt(newAge);
        final String query = "MATCH (n:Person {name:'" + oldName + "'}) SET n.name = '" + newName + "', n.age = " + age + " RETURN n;";
        send(query,true);
    }

    /* Get all nodes with the specified name */
    public void read(final String name) {
        System.out.println("NODES:");
        final String query1 = "MATCH (n:Person {name:'" + name + "'}) RETURN n;";
        send(query1,true);

        System.out.println("RELATIONSHIPS:");
        final String query2 = "MATCH (n:Person {name:'" + name + "'})-[r]->() RETURN r;";
        send(query2,true);
    }

}
