import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

//import java.util.*;

import javax.ws.rs.core.MediaType;


public class TxHandler {

    private String SERVER_ROOT_URI;


    public TxHandler() {
        this.SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    }

    private void send(final String query) {

        final String txUri = SERVER_ROOT_URI + "transaction/commit/";

        // Establish connection to the resource
        WebResource resource = Client.create().resource(txUri);

        // Create data to send
        String payload = "{\"statements\" : [ {\"statement\" : \"" + query + "\"} ]}";

        // POST
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON)
                .entity( payload )
                .post( ClientResponse.class );

        /*
        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: " + System.lineSeparator() + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity( String.class ) ));
        */

        if(response.getStatus() == 200) {
            System.out.println("Query successful.");
            System.out.println(response.getEntity( String.class ));
        }

        System.out.println();

        response.close();

    }

    public void query(final String query) {
        send(query);
    }


    public void add(final String name, final String ageStr) {
        int age = Integer.parseInt(ageStr);
        final String query = "CREATE (new:Person {name:'" + name + "', age:" + age + "}) RETURN new;";
        send(query);
    }


    public void delete(final String name) {
        final String query = "MATCH (node:Person {name:'" + name + "'}) DETACH DELETE node;";
        send(query);
    }

    public void update(final String oldName, final String newName, final String newAge) {
        int age = Integer.parseInt(newAge);
        final String query = "MATCH (node:Person {name:'" + oldName + "'}) SET node.name = '" + newName + "', node.age = " + age + " RETURN node;";
        send(query);
    }

    public void read(final String name) {
        final String query = "MATCH (node:Person {name:'" + name + "'}) RETURN node;";
        send(query);
    }




}
