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
        createDB();
        populateDB();
    }

    // runs the DB (incomplete)
    public void run() {
        queryTeamSQLTest();
        queryTeamPitchingSQLTest();
        queryTeamBattingSQLTest();
        queryTeamFieldingSQLTest();
        queryManagerSQLTest();
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

    private void createDB() {
        System.out.println("Creating DB...");
        try {
            String fileContents = "";
            Scanner s = new Scanner(new File("./baseball_data_server.sql"));
            while(s.hasNextLine()) fileContents += s.nextLine() +"\n";
            statement.execute(fileContents);
            System.out.println("Creation Successful!");
        }
        catch(Exception e) {
            //e.printStackTrace();
            System.out.println("Unable to create DB. Quitting...");
            System.exit(0);
        }
    }

    // POPULATION METHODS
    private void populateDB() {
        try {
            System.out.println("Populating the DB...");
            statement.execute(populateDBSql());
            System.out.println("Population Complete!");
        }
        catch(Exception e) {
            //e.printStackTrace();
            System.out.println("Unable to populate DB. Quitting...");
            System.exit(0);
        }
        
    }

    private String populateDBSql() {
        String res = "";
        res += populateDivisionSQL();
        res += populateTeamsSQL();
        res += populateTeamPitchingSQL();
        res += populateTeamBattingSQL();
        res += populateTeamFieldingSQL();
        res += populateManagerSQL();
        return res;
    }

    private String populateDivisionSQL() {
        return "INSERT INTO division (divisionName, abbreviation) VALUES('American League East', 'ALE')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('American League Central', 'ALC')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('American League West', 'ALW')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('National League East', 'NLE')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('National League Central', 'NLC')\n" +
        "INSERT INTO division (divisionName, abbreviation) VALUES('National League West', 'NLW')\n";
    }

    private String populateTeamsSQL() {
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
                insertSql += String.format("INSERT INTO team (teamName, abbrev, city, divisionName) VALUES('%s', '%s', '%s', '%s')\n", teamInfo[1], teamInfo[2], teamInfo[3], league+" "+teamInfo[0]);
            }
            return insertSql;
        } 
        catch(Exception e) {
            System.out.println("Unable to load Team table");
            return null;
        }
    }

    private String populateTeamPitchingSQL() {
        try {
            String insertSql = "";
            ArrayList<String> tpsLine = new ArrayList<>();
            Scanner s = new Scanner(new File("./3380ProjectData/TeamPitching.csv"));
            while(s.hasNextLine()) tpsLine.add(s.nextLine());

            String outline = "INSERT INTO teamPitchingStats (totalIP, totalER, totalStrikeOuts, totalWalksAllowed, totalHRAllowed, totalHitsAllowed, teamName) VALUES('%f', '%d', '%d', '%d', '%d', '%d', '%s')\n";
            for(int i = 1; i < tpsLine.size(); i++) {
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

    private String populateTeamBattingSQL() {
        try {
            String insertSql = "";
            ArrayList<String> tbsLine = new ArrayList<>();
            Scanner s = new Scanner(new File("./3380ProjectData/TeamBatting.csv"));
            while(s.hasNextLine()) tbsLine.add(s.nextLine());

            String outline = "INSERT INTO teamBattingStats (totalHR, totalHits, totalAB, teamName) VALUES('%d', '%d', '%d', '%s')\n";
            for(int i = 1; i < tbsLine.size(); i++) {
                String[] tbsInfo = tbsLine.get(i).split(",");
                insertSql += String.format(outline, stoi(tbsInfo[11]), stoi(tbsInfo[8]), stoi(tbsInfo[6]), tbsInfo[0]);
            }
            return insertSql;
        }
        catch(Exception e) {
            System.out.println("Unable to load TeamPitching table");
            return null;
        }
    }

    private String populateTeamFieldingSQL() {
        try {
            String insertSql = "";
            ArrayList<String> tfsLine = new ArrayList<>();
            Scanner s = new Scanner(new File("./3380ProjectData/TeamFielding.csv"));
            while(s.hasNextLine()) tfsLine.add(s.nextLine());

            String outline = "INSERT INTO teamFieldingStats (totalPutouts, totalAssists, totalErrors, doublePlays, teamName) VALUES('%d', '%d', '%d', '%d', '%s')\n";
            for(int i = 1; i < tfsLine.size(); i++) {
                String[] tfsInfo = tfsLine.get(i).split(",");
                insertSql += String.format(outline, stoi(tfsInfo[9]), stoi(tfsInfo[10]), stoi(tfsInfo[11]), stoi(tfsInfo[12]), tfsInfo[0]);
            }
            return insertSql;
        }
        catch(Exception e) {
            System.out.println("Unable to load TeamFielding table");
            return null;
        }
    }

    private String populateManagerSQL() {
        try {
            String insertSql = "";
            ArrayList<String> managerLine = new ArrayList<>();
            Scanner s = new Scanner(new File("./3380ProjectData/Managers.csv"));
            while(s.hasNextLine()) managerLine.add(s.nextLine());

            String outline = "INSERT INTO manager (managerName, ejections, challanges, overturned, teamAbbrev) VALUES('%s', '%d', '%d', '%d', '%s')\n";
            for(int i = 1; i < managerLine.size(); i++) {
                String[] managerInfo = managerLine.get(i).split(",");
                insertSql += String.format(outline, managerInfo[1], stoi(managerInfo[15]), stoi(managerInfo[12]), stoi(managerInfo[13]), managerInfo[2]);
            }
            return insertSql;
        }
        catch(Exception e) {
            System.out.println("Unable to load Manager table");
            // e.printStackTrace();
            return null;
        }
    }

    // RUN QUERY METHODS
    private void queryTeamSQLTest() {
        System.out.println("\nTeam Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamName, abbrev from team;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamName\t\t\tAbbreviation");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%s\n", resultSet.getString("teamName"), resultSet.getString("abbrev"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
        }
    }

    private void queryTeamPitchingSQLTest() {
        System.out.println("\nTeam Pitching Query\n");
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
    
    private void queryTeamBattingSQLTest() {
        System.out.println("\nTeam Batting Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamName, totalHits from teamBattingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamName\t\t\ttotalHits");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%s\n", resultSet.getString("teamName"), resultSet.getString("totalHits"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
        }
    }

    private void queryTeamFieldingSQLTest() {
        System.out.println("\nTeam Fielding Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamName, totalErrors from teamFieldingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamName\t\t\ttotalErrors");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%s\n", resultSet.getString("teamName"), resultSet.getString("totalErrors"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
        }
    }

    private void queryManagerSQLTest() {
        System.out.println("\nManager Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT managerName, teamAbbrev from manager;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("Name\t\t\tTeam Abbrev");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%s\n", resultSet.getString("managerName"), resultSet.getString("teamAbbrev"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
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
