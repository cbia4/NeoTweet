import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.util.*;
import javax.ws.rs.core.MediaType;


public class NeoClient {
    // Base URI to listen to
    public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

    public static void main(String[] args) {


        Scanner sc = new Scanner(System.in);
        String query = "";

        System.out.println("Welcome to the Neo4j client!");
        System.out.println("NOTE: Make sure to use single quotes when typing queries!");


        while(!query.equals("quit")) {
            System.out.print("> ");
            query = sc.nextLine();

            if(!query.equals("quit")) {
                System.out.println();
                cypherQuery(query);
            }

        }


        java.net.URI firstNode = createNode();
        addProperty(firstNode, "name", "Colin Biafore");

    }

    public static void cypherQuery(final String query) {

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

        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: " + System.lineSeparator() + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity( String.class ) ));

        System.out.println();

        response.close();



    }

    public static java.net.URI createNode() {

        final String nodeEntryPointUri = SERVER_ROOT_URI + "node/";

        WebResource resource = Client.create()
                .resource( nodeEntryPointUri );

        // POST {} to the node entry point URI
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( "{}" )
                .post( ClientResponse.class );

        final java.net.URI location = response.getLocation();

        System.out.println(String.format(
                "POST to [%s], status code[%d], location header [%s]",
                nodeEntryPointUri, response.getStatus(), location.toString() ));

        response.close();

        return location;

    }

    public static void addProperty(java.net.URI nodeUri, String propertyName, String propertyValue) {

        String propertyUri = nodeUri.toString() + "/properties/" + propertyName;
        // http://localhost:7474/db/data/node/{node_id}/properties/{property_name}

        WebResource resource = Client.create()
                .resource( propertyUri );

        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type(MediaType.APPLICATION_JSON)
                .entity( "\"" + propertyValue + "\"")
                .put( ClientResponse.class );

        System.out.println( String.format(
                "PUT to [%s], status code [%d]",
                propertyUri, response.getStatus() ));

        response.close();

    }

}
