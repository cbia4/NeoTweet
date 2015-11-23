/**
 * See http://neo4j.com/docs/stable/server-java-rest-client-example.html
 *   for the correct way to send a Cypher statement to RESTful Neo4j.
 *   
 * See http://neo4j.com/docs/stable/rest-api-transactional.html
 *   for information about the transactional Cypher HTTP endpoint.
 * 
 * 
 * 
 */

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;



public class NeoClientFN {
    /* We are using the Transactional Cypher HTTP enpoint */
    public static final String SERVER_ROOT_URI = 
    		"http://localhost:7474/db/data/transaction/commit";

    public static void main(String[] args) {

    	WebResource resource = Client.create().resource(SERVER_ROOT_URI);
    	
    	/* the Cypher query */
    	String query = "MATCH (a:Person) RETURN a";
    	
    	/* The payload in josn format */
    	String payload = "{\"statements\" :"
    			+ "[ {\"statement\" :"
    			+ "\""+ query + "\"} ] }";
    	
    	ClientResponse response = resource
    	        .accept( MediaType.APPLICATION_JSON )
    	        .type( MediaType.APPLICATION_JSON )
    	        .entity( payload )
    	        .post( ClientResponse.class );
    	
    	System.out.println( String.format(
    	        "POST [%s] to [%s], status code [%d], returned data: "
    	                + System.lineSeparator() + "%s",
    	        payload, SERVER_ROOT_URI, response.getStatus(),
    	        response.getEntity( String.class ) ) );

    	response.close();
    }

}
