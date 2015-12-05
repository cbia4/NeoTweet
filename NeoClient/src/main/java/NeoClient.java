
import java.util.*;




public class NeoClient {

    public static void main(String[] args) {

        TxHandler neoTx = new TxHandler();
        Scanner sc = new Scanner(System.in);
        String input = "";
        String[] inputSplit;

        System.out.println("-----------------------------------");
        System.out.println("Welcome to the Neo4j Client!");
        System.out.println("Options:");
        System.out.println("ADD <name> <age>");
        System.out.println("DELETE <name>");
        System.out.println("UPDATE <oldname> <newname> <newage>");
        System.out.println("READ <name>");
        System.out.println("QUERY (cypher query)");
        System.out.println("EXIT (quit client)");
        System.out.println("-----------------------------------");
        System.out.println();

        while(!input.equals("EXIT")) {
            System.out.print("> ");
            input = sc.nextLine();
            inputSplit = input.split(" ");

            if( inputSplit[0].equals("ADD") ) {
                neoTx.add(inputSplit[1],inputSplit[2]);
            }

            else if( inputSplit[0].equals("DELETE") ) {
                neoTx.delete(inputSplit[1]);
            }

            else if( inputSplit[0].equals("UPDATE") ) {
                neoTx.update(inputSplit[1], inputSplit[2], inputSplit[3]);
            }

            else if( inputSplit[0].equals("READ") ) {
                neoTx.read(inputSplit[1]);
            }

            else if ( inputSplit[0].equals("QUERY") ) {
                System.out.print("----> ");
                input = sc.nextLine();
                neoTx.query(input);
            }

            else {
                if(!inputSplit[0].equals("EXIT")) {
                    System.out.println("Error: Invalid input!");
                }
            }



        }


    }

}
