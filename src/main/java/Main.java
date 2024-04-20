import utils.ConnectConfig;
import queries.*;
import utils.DatabaseConnector;
import java.util.Scanner;
import java.util.logging.Logger;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            // parse connection config from "resources/application.yaml"
            ConnectConfig conf = new ConnectConfig();
            log.info("Success to parse connect config. " + conf.toString());
            // connect to database
            DatabaseConnector connector = new DatabaseConnector(conf);
            boolean connStatus = connector.connect();
            if (!connStatus) {
                log.severe("Failed to connect database.");
                System.exit(1);
            }

            // Create an instance of LibraryManagementSystemImpl
            LibraryManagementSystemImpl libraryManagementSystem = new LibraryManagementSystemImpl(connector);

            // Create a Scanner to read user input
            Scanner scanner = new Scanner(System.in);

            // User interaction loop
            while (true) {
                System.out.println("Enter a command:");
                String command = scanner.nextLine();

                // Perform different actions based on the command
                if (command.equals("showCards")) {
                    ApiResult result = libraryManagementSystem.showCards();
                    System.out.println(result);
                } else if (command.equals("resetDatabase")) {
                    ApiResult result = libraryManagementSystem.resetDatabase();
                    System.out.println(result);
                } else if (command.equals("exit")) {
                    break;
                } else {
                    System.out.println("Unknown command: " + command);
                }
            }

            // release database connection handler
            if (connector.release()) {
                log.info("Success to release connection.");
            } else {
                log.warning("Failed to release connection.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}