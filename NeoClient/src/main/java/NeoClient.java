import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;



public class NeoClient {
    // Base URI to listen to
    public static final String SERVER_ROOT_URI = "http://localhost:7474/browser/";

    public static void main(String[] args) {

        WebResource resource = Client.create().resource(SERVER_ROOT_URI);

        ClientResponse response = resource.get(ClientResponse.class);

        System.out.println( String.format("GET on [%s], status code [%d]",
                SERVER_ROOT_URI, response.getStatus()));

        response.close();

    }

}
