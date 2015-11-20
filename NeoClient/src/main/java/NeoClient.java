import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import javax.ws.rs.core.MediaType;


public class NeoClient {
    // Base URI to listen to
    public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

    public static void main(String[] args) {


        WebResource resource = Client.create().resource(SERVER_ROOT_URI);

        ClientResponse response = resource.get(ClientResponse.class);

        System.out.println( String.format("GET on [%s], status code [%d]",
                SERVER_ROOT_URI, response.getStatus()));

        response.close();



        final String txUri = SERVER_ROOT_URI + "transaction/commit/";
        final String query = "START n=node(*) MATCH n WHERE HAS(n.name) RETURN n;";
        resource = Client.create().resource( txUri );
        String payload = "{\"statements\" : [ {\"statement\" : \"" + query + "\"} ]}";

         response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( payload )
                .post ( ClientResponse.class );

        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                + System.lineSeparator() + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity( String.class )
        ));

        response.close();




    }

}
