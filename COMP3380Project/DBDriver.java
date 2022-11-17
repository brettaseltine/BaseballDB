import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class DBDriver {
    
    public static void main(String[] args) {
        BaseballDB bdb = new BaseballDB();
        bdb.run();
    }
}

class BaseballDB {

    Connection connection;
    Statement statement;

    // constructor
    public BaseballDB() {
        connect();
        populateDB();
    }

    // runs the DB (incomplete)
    public void run() {
        queryTPSSQL();
    }

    // Connect to your database.
    private void connect() {
        System.out.println("Connecting to DB...");

        Properties prop = new Properties();
        String fileName = "auth.cfg";
        try {
            FileInputStream configFile = new FileInputStream(fileName);
            prop.load(configFile);
            configFile.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Could not find config file.");
            System.exit(1);
        } catch (IOException ex) {
            System.out.println("Error reading config file.");
            System.exit(1);
        }
        String username = (prop.getProperty("username"));
        String password = (prop.getProperty("password"));

        if (username == null || password == null){
            System.out.println("Username or password not provided.");
            System.exit(1);
        }

        String connectionUrl =
                "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                + "database=cs3380;"
                + "user=" + username + ";"
                + "password="+ password +";"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        try {
            this.connection = DriverManager.getConnection(connectionUrl);
            this.statement = connection.createStatement();
            System.out.println("Connection Successful!");
        }
        catch (SQLException e) {
            //e.printStackTrace();
            System.out.println("Unable to connect to DB. Quitting...");
            System.exit(0);
        }
    }

    // POPULATION METHODS
    private void populateDB() {
        try {
            System.out.println("Populating the DB...");
            statement.execute(getPopulateDBSql());
            System.out.println("Population Successful!");
        }
        catch(Exception e) {
            //e.printStackTrace();
            System.out.println("Unable to populate DB. Quitting...");
            System.exit(0);
        }
        
    }

    private String getPopulateDBSql() {
        String res = "";
        res += getPopulateDivisionSQL();
        res += getPopulateTeamsSQL();
        res += getPopulateTeamPitchingSQL();
        return res;
    }

    private String getPopulateDivisionSQL() {
        return "INSERT INTO division (divisionName, abbreviation) VALUES('American League East', 'ALE')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('American League Central', 'ALC')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('American League West', 'ALW')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('National League East', 'NLE')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('National League Central', 'NLC')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('National League West', 'NLW')\n";
    }

    private String getPopulateTeamsSQL() {
        try{
            String insertSql = "";
            ArrayList<String> teamLine = new ArrayList<>();
            Scanner s = new Scanner(new File("./3380ProjectData/Teams.csv"));
            while(s.hasNextLine()) teamLine.add(s.nextLine());

            String league = "American League";
            for(int i = 2; i < teamLine.size(); i++) {
                if(i == 17) {
                    league = "National League";
                    continue;
                }
                String[] teamInfo = teamLine.get(i).split(",");
                insertSql += String.format("INSERT INTO team (teamName, city, divisionName) VALUES('%s', '%s', '%s')\n", teamInfo[1], teamInfo[2], league+" "+teamInfo[0]);
            }
            return insertSql;
        } 
        catch(Exception e) {
            System.out.println("Unable to load Team table");
            return null;
        }
    }

    private String getPopulateTeamPitchingSQL() {
        try {
            String insertSql = "";
            ArrayList<String> tpsLine = new ArrayList<>();
            Scanner s = new Scanner(new File("./3380ProjectData/TeamPitching.csv"));
            while(s.hasNextLine()) tpsLine.add(s.nextLine());

            String outline = "INSERT INTO teamPitchingStats (totalIP, totalER, totalStrikeOuts, totalWalksAllowed, totalHRAllowed, totalHitsAllowed, teamName) VALUES('%f', '%d', '%d', '%d', '%d', '%d', '%s')\n";
            for(int i = 2; i < tpsLine.size(); i++) {
                String[] tpsInfo = tpsLine.get(i).split(",");
                insertSql += String.format(outline, stof(tpsInfo[15]), stoi(tpsInfo[18]), stoi(tpsInfo[22]), stoi(tpsInfo[20]), stoi(tpsInfo[19]), stoi(tpsInfo[16]), tpsInfo[0]);
            }
            return insertSql;
        }
        catch(Exception e) {
            System.out.println("Unable to load TeamPitching table");
            return null;
        }
    }

    // RUN QUERY METHODS
    private void queryTPSSQL() {
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamName, totalIP from teamPitchingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamName\t\t\ttotalIP");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%s\n", resultSet.getString("teamName"), resultSet.getString("totalIP"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
        }
        
    }
    
    // Helper Methods
    private int stoi(String s) {
        return Integer.parseInt(s);
    }
    private float stof(String s) {
        return Float.parseFloat(s);
    }
}
