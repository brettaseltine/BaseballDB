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
    Map<String, Integer> playerMap;
    Map<String, Integer> teamMap;

    // constructor
    public BaseballDB() {
        playerMap = new HashMap<>();
        teamMap = new HashMap<>();

        connect();
        // createDB();
        // populateDB();
    }

    // runs the DB
    public void run() {
        runInterface();
    }

    private void runInterface() {
        Scanner console = new Scanner(System.in);
		String[] parts;
		String arg = "";
        String prompt = "MLB DB > ";

        System.out.println("Welcome to the MLB Database! Type h for help. ");
        System.out.print(prompt);
        String line = console.nextLine();

		while (line != null && !line.equals("q")) {
			parts = line.split(" ");
			if (line.indexOf(" ") > 0)
				arg = line.substring(line.indexOf(" ")).trim();

			if (parts[0].equals("h")) {
                printHelp();
            } else if (parts[0].equals("t")) {
				showTeamInfo();
			} else if (parts[0].equals("allPlayers")) {
                showAllPlayers();
            } else if (parts[0].equals("divs")) {
                showAllDivision();
            } else if (parts[0].equals("pitchingStats")) {
                if(parts.length >= 2) {
                    showPitchingStats(arg);
                } else {
                    System.out.println("This command requires an argument");
                }
                
            }

			System.out.print("\n\n"+prompt);
			line = console.nextLine();
		}

		console.close();
	}

    private static void printHelp() {
		System.out.println("Commands:");
		System.out.println("h - Get help");
		System.out.println("t - show team information");
        System.out.println("allPlayers - show all players");
        System.out.println("divs - show all divisions");
        System.out.println("pitchingStats <playerName> - show players pitching stats");

		
		System.out.println("");

		System.out.println("q - Exit the program");
		System.out.println("---- end help ----- ");
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
            e.printStackTrace();
            System.out.println("Unable to create DB. Quitting...");
            System.exit(0);
        }
    }

    // POPULATION METHODS
    private void populateDB() {
        System.out.println("Populating the DB...");
        populateDivision();
        populateTeam();
        populateTeamPitching();
        populateTeamBatting();
        populateTeamFielding();
        populateManager();
        populatePlayer();
        populatePitchingStats();
        populateBattingStats();
        populateFieldingStats();
        populatePlaysOn();
        System.out.printf("Population Complete!");
        
    }

    private void populateDivision() {
        try {
            String insertSQL = "INSERT INTO division (divisionName, abbreviation) VALUES('American League East', 'ALE')\n" +
                               "INSERT INTO division (divisionName, abbreviation) VALUES('American League Central', 'ALC')\n" +
                               "INSERT INTO division (divisionName, abbreviation) VALUES('American League West', 'ALW')\n" +
                               "INSERT INTO division (divisionName, abbreviation) VALUES('National League East', 'NLE')\n" +
                               "INSERT INTO division (divisionName, abbreviation) VALUES('National League Central', 'NLC')\n" +
                               "INSERT INTO division (divisionName, abbreviation) VALUES('National League West', 'NLW')\n";
            statement.execute(insertSQL);
        }
        catch(Exception e) {
            System.out.println("Unable to populate division table");
        }
    }

    private void populateTeam() {
        try{
            String insertSql = "";
            ArrayList<String> teamLine = getFileLines("./3380ProjectData/Teams.csv");

            String league = "American League";
            for(int i = 2; i < teamLine.size(); i++) {
                if(i == 17) {
                    league = "National League";
                    continue;
                }
                String[] teamInfo = teamLine.get(i).split(",");
                insertSql += String.format("INSERT INTO team (teamName, abbrev, city, divisionName) VALUES('%s', '%s', '%s', '%s')\n", teamInfo[1], teamInfo[2], teamInfo[3], league+" "+teamInfo[0]);
            }
            statement.execute(insertSql);
            // save the id
        } 
        catch(Exception e) {
            System.out.println("Unable to load Team table");
        }
    }

    private void populateTeamPitching() {
        try {
            String insertSql = "";
            ArrayList<String> tpsLine = getFileLines("./3380ProjectData/TeamPitching.csv");

            String outline = "INSERT INTO teamPitchingStats (totalIP, totalER, totalStrikeOuts, totalWalksAllowed, totalHRAllowed, totalHitsAllowed, teamID) VALUES('%f', '%d', '%d', '%d', '%d', '%d', '%d')\n";
            for(int i = 1; i < tpsLine.size(); i++) {
                String[] tpsInfo = tpsLine.get(i).split(",");
                int teamID = getTeamID(tpsInfo[0]);
                insertSql += String.format(outline, stof(tpsInfo[15]), stoi(tpsInfo[18]), stoi(tpsInfo[22]), stoi(tpsInfo[20]), stoi(tpsInfo[19]), stoi(tpsInfo[16]), teamID);
            }
            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load TeamPitching table");
            e.printStackTrace();
        }
    }

    private void populateTeamBatting() {
        try {
            String insertSql = "";
            ArrayList<String> tbsLine = getFileLines("./3380ProjectData/TeamBatting.csv");

            String outline = "INSERT INTO teamBattingStats (totalHR, totalHits, totalAB, teamID) VALUES('%d', '%d', '%d', '%d')\n";
            for(int i = 1; i < tbsLine.size(); i++) {
                String[] tbsInfo = tbsLine.get(i).split(",");
                int teamID = getTeamID(tbsInfo[0]);
                insertSql += String.format(outline, stoi(tbsInfo[11]), stoi(tbsInfo[8]), stoi(tbsInfo[6]), teamID);
            }
            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load TeamPitching table");
        }
    }

    private void populateTeamFielding() {
        try {
            String insertSql = "";
            ArrayList<String> tfsLine = getFileLines("./3380ProjectData/TeamFielding.csv");

            String outline = "INSERT INTO teamFieldingStats (totalPutouts, totalAssists, totalErrors, doublePlays, teamID) VALUES('%d', '%d', '%d', '%d', '%d')\n";
            for(int i = 1; i < tfsLine.size(); i++) {
                String[] tfsInfo = tfsLine.get(i).split(",");
                int teamID = getTeamID(tfsInfo[0]);
                insertSql += String.format(outline, stoi(tfsInfo[9]), stoi(tfsInfo[10]), stoi(tfsInfo[11]), stoi(tfsInfo[12]), teamID);
            }
            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load TeamFielding table");
        }
    }

    private void populateManager() {
        try {
            String insertSql = "";
            ArrayList<String> managerLine = getFileLines("./3380ProjectData/Managers.csv");

            String outline = "INSERT INTO manager (managerName, ejections, challanges, overturned, teamID) VALUES('%s', '%d', '%d', '%d', '%d')\n";
            for(int i = 1; i < managerLine.size(); i++) {
                String[] managerInfo = managerLine.get(i).split(",");
                int teamID = getTeamID(managerInfo[2]);
                insertSql += String.format(outline, managerInfo[1], stoi(managerInfo[15]), stoi(managerInfo[12]), stoi(managerInfo[13]), teamID);
            }
            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load Manager table");
        }
    }

    private void populatePlayer() {
        // note: player name --> playerID (no 2 players have the same name)

        try {
            Set<String> playerNames = new HashSet<>();
            String insertSql = "";
            String outline = "INSERT INTO Player (name, age) VALUES('%s', %d)\n";

            ArrayList<String> pitcher = getFileLines("./3380ProjectData/PlayerPitching.csv");
            ArrayList<String> batter = getFileLines("./3380ProjectData/PlayerBatting.csv");
            ArrayList<String> fielder = getFileLines("./3380ProjectData/PlayerFielding.csv");


            // add batter if not already added
            insertSql += playerInsertStr(pitcher, playerNames, outline);
            insertSql += playerInsertStr(batter, playerNames, outline);
            insertSql += playerInsertStr(fielder, playerNames, outline);

            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load Player table");
        }
    }

    private void populatePitchingStats() {
        try {
            String insertSql = "";
            ArrayList<String> statLine = getFileLines("./3380ProjectData/PlayerPitching.csv");

            String outline = "INSERT INTO pitchingStats "+
                             "(teamAbbrev, gamesPlayed, hitsAllowed, hrAllowed, walksAllowed, strikeOuts, inningsPitched, earnedRuns, playerID)" +
                             " VALUES('%s', '%d', '%d', '%d', '%d', '%d', '%f', '%d', '%d')\n";
            for(int i = 1; i < statLine.size(); i++) {
                String[] statInfo = statLine.get(i).split(",");
                String name = formatName(statInfo[1]);
                int playerID = getPlayerID(name);
                insertSql += String.format(outline, statInfo[3], stoi(statInfo[9]), stoi(statInfo[16]), stoi(statInfo[19]), stoi(statInfo[20]), 
                                                    stoi(statInfo[22]), stof(statInfo[15]), stoi(statInfo[18]), playerID);
            }
            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load PitchingStats table");
            e.printStackTrace();
        }
    }
    
    private void populateBattingStats() {
        try {
            String insertSql = "";
            ArrayList<String> statLine = getFileLines("./3380ProjectData/PlayerBatting.csv");

            String outline = "INSERT INTO battingStats (hrHit, hits, atBats, playerID) VALUES('%d', '%d', '%d', '%d')\n";
            for(int i = 1; i < statLine.size(); i++) {
                String[] statInfo = statLine.get(i).split(",");
                String name = formatName(statInfo[1]);
                int playerID = getPlayerID(name);
                insertSql += String.format(outline, stoi(statInfo[12]), stoi(statInfo[9]), stoi(statInfo[7]), playerID);
            }
            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load BattingStats table");
            e.printStackTrace();
        }
    }

    private void populateFieldingStats() {
        try {
            String insertSql = "";
            ArrayList<String> statLine = getFileLines("./3380ProjectData/PlayerFielding.csv");

            String outline = "INSERT INTO fieldingStats (putouts, assists, errors, playerID) VALUES('%d', '%d', '%d', '%d')\n";
            for(int i = 1; i < statLine.size(); i++) {
                String[] statInfo = statLine.get(i).split(",");
                String name = formatName(statInfo[1]);
                int playerID = getPlayerID(name);
                insertSql += String.format(outline, stoi(statInfo[10]), stoi(statInfo[11]), stoi(statInfo[12]), playerID);
            }
            statement.execute(insertSql);
        }
        catch(Exception e) {
            System.out.println("Unable to load FieldingStats table");
            e.printStackTrace();
        }
    }

    private void populatePlaysOn() {
        try {
            // for each player in player files
                // get playerID from name
                // get teamID from abbrev
                // add them to table
            Set<String> playerNames = new HashSet<>();
            String insertSql = "";
            String outline = "INSERT INTO PlaysOn (playerID, teamID) VALUES(%d, %d)\n";

            ArrayList<String> pitchers = getFileLines("./3380ProjectData/PlayerPitching.csv");
            ArrayList<String> batters = getFileLines("./3380ProjectData/PlayerBatting.csv");
            ArrayList<String> fielders = getFileLines("./3380ProjectData/PlayerFielding.csv");

            insertSql += playsOnInsertStr(pitchers, playerNames, outline);
            insertSql += playsOnInsertStr(batters, playerNames, outline);
            insertSql += playsOnInsertStr(fielders, playerNames, outline);

            statement.execute(insertSql);
        }
        catch (Exception e){
            System.out.println("Unable to populate playsOn table");
        }
    }

    // TEST QUERY METHODS
    private void queryTeamTest() {
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

    private void queryTeamPitchingTest() {
        System.out.println("\nTeam Pitching Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamID, totalIP from teamPitchingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamID\t\t\ttotalIP");    // testing line
            while (resultSet.next()) {
                System.out.printf("%d\t\t\t%d\n", resultSet.getInt("teamID"), resultSet.getInt("totalIP"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
        }
        
    }
    
    private void queryTeamBattingTest() {
        System.out.println("\nTeam Batting Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamID, totalHits from teamBattingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamID\t\t\ttotalHits");    // testing line
            while (resultSet.next()) {
                System.out.printf("%d\t\t\t%d\n", resultSet.getInt("teamID"), resultSet.getInt("totalHits"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
        }
    }

    private void queryTeamFieldingTest() {
        System.out.println("\nTeam Fielding Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamID, totalErrors from teamFieldingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamID\t\t\ttotalErrors");    // testing line
            while (resultSet.next()) {
                System.out.printf("%d\t\t\t%d\n", resultSet.getInt("teamID"), resultSet.getInt("totalErrors"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }

    private void queryManagerTest() {
        System.out.println("\nManager Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT managerName, teamID from manager;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("Name\t\t\tTeamID");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%d\n", resultSet.getString("managerName"), resultSet.getInt("teamID"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }

    private void queryPlayerTest() {
        System.out.println("\nPlayer Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT playerID, name, age from player;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("playerID\t\t\tName\t\t\tAge");    // testing line
            while (resultSet.next()) {
                System.out.printf("%d\t\t\t%s\t\t\t%s\n", resultSet.getInt("playerID"), resultSet.getString("name"), resultSet.getInt("age"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }

    private void queryPitchingStatsTest() {
        System.out.println("\nPitching Stats Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT teamAbbrev, inningsPitched from pitchingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("TeamAbbrev\t\t\tIP");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%s\n", resultSet.getString("teamAbbrev"), resultSet.getString("inningsPitched"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }

    private void queryBattingStatsTest() {
        System.out.println("\nBatting Stats Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT battingStatsID, hits from battingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("BattingID\t\t\thits");    // testing line
            while (resultSet.next()) {
                System.out.printf("%d\t\t\t%d\n", resultSet.getInt("battingStatsID"), resultSet.getInt("hits"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }

    private void queryFieldingStatsTest() {
        System.out.println("\nFielding Stats Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT fieldingStatsID, errors from fieldingStats;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("FieldingID\t\t\tErrors");    // testing line
            while (resultSet.next()) {
                System.out.printf("%d\t\t\t%d\n", resultSet.getInt("fieldingStatsID"), resultSet.getInt("errors"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }

    private void queryPlaysOnTest() {
        System.out.println("\nPlaysOn Query\n");
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT name, teamName from playsOn "+
                                "join player on playsOn.playerID = player.playerID "+
                                "join team on playsOn.teamID = team.teamID;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.println("Player Name\t\t\tTeam Name");    // testing line
            while (resultSet.next()) {
                System.out.printf("%s\t\t\t%s\n", resultSet.getString("name"), resultSet.getString("teamName"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }

    private int queryPlayerID(String name) {
        try {
            String selectSql = String.format("SELECT playerID from player where player.name = '%s';", name);    
            ResultSet resultSet = statement.executeQuery(selectSql);
            return resultSet.next() ? resultSet.getInt("playerID") : -1;
        }
        catch(Exception e) {
            System.out.println("Failed to find player with name: " + name);
            e.printStackTrace();
            return -1;
        }
    }

    private int queryTeamID(String identifier) {
        try {
            String selectSql = String.format("SELECT teamID from team where team.teamName = '%s' or team.abbrev = '%s';", identifier, identifier);    
            ResultSet resultSet = statement.executeQuery(selectSql);
            return resultSet.next() ? resultSet.getInt("teamID") : -1;
        }
        catch(Exception e) {
            System.out.println("Failed to find team with name: " + identifier);
            e.printStackTrace();
            return -1;
        }
    }

    // INTERFACE QUERIES
    private void showTeamInfo() {
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "select teamName, abbrev, city, divisionName from team order by teamName;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.printf("%-30s %-10s %-20s %-10s\n", "Team Name", "Abbrev", "City", "Divison");
            while (resultSet.next()) {
                System.out.printf("%-30s %-10s %-20s %-10s\n", resultSet.getString("teamName"), resultSet.getString("abbrev"), resultSet.getString("city"), resultSet.getString("divisionName"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            // e.printStackTrace();
        }
    }
    private void showAllPlayers() {
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "select name, age from player order by name;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.printf("%-32s %s\n", "Name", "Age");
            while (resultSet.next()) {
                System.out.printf("%-32s %d\n", resultSet.getString("name"), resultSet.getInt("age"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }
    private void showAllDivision() {
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = "select * from division order by divisionName;";
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.printf("%-35s %s\n", "Division Name", "Abbrev");
            while (resultSet.next()) {
                System.out.printf("%-35s %s\n", resultSet.getString("divisionName"), resultSet.getString("abbreviation"));
            }
        }
        catch(Exception e) {
            System.out.println("query failed");
            e.printStackTrace();
        }
    }
    private void showPitchingStats(String name) {
        try {
            // Create and execute a SELECT SQL statement.
            String selectSql = String.format("select player.name, teamAbbrev, gamesPlayed, hitsAllowed, hrAllowed, walksAllowed, strikeOuts, inningsPitched, earnedRuns from pitchingStats "+
            "join player on pitchingStats.playerID = player.playerID where player.name = '%s';", name);
            ResultSet resultSet = statement.executeQuery(selectSql);

            // Print results from select statement
            System.out.printf("%-35s %-5s %-15s %-15s %-15s %-15s %-15s %-15s %-15s\n",
                             "Name", "Team", "Games Played", "Hits Allowed", "HR Allowed", "Walks Allowed", "Strike Outs", "IP", "ER");
            if (resultSet.next()) {
                System.out.printf("%-35s %-5s %-15d %-15d %-15d %-15d %-15d %-15d %-15d\n", 
                    resultSet.getString("name"), resultSet.getString("teamAbbrev"), resultSet.getInt("gamesPlayed"),
                    resultSet.getInt("hitsAllowed"), resultSet.getInt("hrAllowed"), resultSet.getInt("walksAllowed"),
                    resultSet.getInt("strikeOuts"), resultSet.getInt("inningsPitched"), resultSet.getInt("earnedRuns"));
            } else {
                System.out.println("Player not found");
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
    private ArrayList<String> getFileLines(String path) {
        try {
            ArrayList<String> lines = new ArrayList<>();
            Scanner s = new Scanner(new File(path));
            while(s.hasNextLine()) lines.add(s.nextLine());
            s.close();
            return lines;
        }
        catch(Exception e) {
            System.out.println("Could not open from:" + path);
            return null;
        }

    }
    private String formatName(String name) {
        String res = "";
        for(int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if((int) c == 160) c = (char) 32;   // fix non-breaking spaces
            if(Character.isLetter(c) || c == ' ')
                res += c;
        }
        return res;
    }
    private String playerInsertStr(ArrayList<String> playerType, Set<String> playerNames, String outline) {
        String res = "";
        for(int i = 1; i < playerType.size(); i++) {
            String[] playerInfo = playerType.get(i).split(",");
            String name = formatName(playerInfo[1]);
            if( !playerNames.contains(name) ) {
                playerNames.add(name);
                res += String.format(outline, name, stoi(playerInfo[2]));
            }
        }
        return res;
    }
    private String playsOnInsertStr(ArrayList<String> playerType, Set<String> playerNames, String outline) {
        String res = "";
        for(int i = 1; i < playerType.size(); i++) {
            String[] playerInfo = playerType.get(i).split(",");
            String name = formatName(playerInfo[1]);
            if( !playerNames.contains(name) ){
                playerNames.add(name);
                int playerID = getPlayerID(name);
                int teamID = getTeamID(playerInfo[3]);
                res += String.format(outline, playerID, teamID);
            }
        }
        return res;
    }
    // Caching
    private int getPlayerID(String name) {
        if(playerMap.containsKey(name)){
            return playerMap.get(name);
        } else {
            int playerID = queryPlayerID(name);
            playerMap.put(name, playerID);
            return playerID;
        }
    }
    private int getTeamID(String identifier){
        if(teamMap.containsKey(identifier)){
            return teamMap.get(identifier);
        } else {
            int teamID = queryTeamID(identifier);
            teamMap.put(identifier, teamID);
            return teamID;
        }
    }

}
