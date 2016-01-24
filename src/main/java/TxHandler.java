import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

//import java.util.*;

import javax.ws.rs.core.MediaType;


public class TxHandler {

    final private String SERVER_ROOT_URI;

    public TxHandler() {

        this.SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    }

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

        /* Capture response status */
        int status = response.getStatus();

        /* Print status or neo4j response */
        if(status != 200) {
            System.out.println("ERROR. Status: " + status);
        } else {
            String neoResponse = response.getEntity(String.class);
            System.out.println("Response: " + neoResponse);
        }

        System.out.println();
        response.close();

    }

    public void query(final String query) {
        send(query);
    }

    public void add(final String name, final String ageStr) {
        int age = Integer.parseInt(ageStr);
        final String query = "CREATE (n:Person {name:'" + name + "', age:" + age + "}) RETURN n;";
        send(query);
    }

    public void addRel(final String name1, final String relType, final String name2) {

        final String query = "MATCH (a:Person),(b:Person) " +
                "WHERE a.name = '" + name1 + "' AND b.name = '" + name2 + "' " +
                "CREATE (a)-[r:" + relType + " {name: a.name + '-[" + relType + "]->' + b.name}]->(b) " +
                "RETURN r;";

        send(query);
    }

    public void delete(final String name) {
        final String query = "MATCH (n:Person {name:'" + name + "'}) DETACH DELETE n;";
        send(query);
    }

    public void update(final String oldName, final String newName, final String newAge) {
        int age = Integer.parseInt(newAge);
        final String query = "MATCH (n:Person {name:'" + oldName + "'}) SET n.name = '" + newName + "', n.age = " + age + " RETURN n;";
        send(query);
    }

    public void read(final String name) {
        System.out.println("NODES:");
        final String query1 = "MATCH (n:Person {name:'" + name + "'}) RETURN n;";
        send(query1);

        System.out.println("RELATIONSHIPS:");
        final String query2 = "MATCH (n:Person {name:'" + name + "'})-[r]->() RETURN r;";
        send(query2);
    }

}
