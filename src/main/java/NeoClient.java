

import java.util.*;




public class NeoClient {

    private TxHandler neoTx;
    private TwitterStream stream;

    public NeoClient() {
        neoTx = new TxHandler();
        stream = new TwitterStream();
    }

    public void printInstructions() {
        System.out.println("--------------------------------------------");
        System.out.println("Options:");
        System.out.println("FETCH <count>");
        System.out.println("READ");
        System.out.println("DELALL (delete all nodes and rels)");
        System.out.println("QUERY (cypher query)");
        System.out.println("EXIT (quit client)");
        System.out.println("--------------------------------------------");
        System.out.println();
    }

    public void getInput() {

        Scanner sc = new Scanner(System.in);
        String input;
        String[] inputString;
        String option;
        boolean inSession = true;

        while(inSession) {
            System.out.print("-> ");
            input = sc.nextLine();
            inputString = input.split(" ");
            option = inputString[0];

            if (option.equals("QUERY")) {
                System.out.print("-->");
                input = sc.nextLine();
                neoTx.query(input);
            }

            else if (option.equals("FETCH")) {
                stream.fetch(inputString[1]);
            }

            else if (option.equals("EXIT")) {
                inSession = false;
            }

            else if (option.equals("DELALL")) {
                neoTx.query("MATCH (n) DETACH DELETE n;");
            }

            else if (option.equals("READ")) {
                neoTx.query("MATCH (n) RETURN n;");
            }

            else {
                System.out.println("Error: Invalid option.");
            }
        }

    }

}
