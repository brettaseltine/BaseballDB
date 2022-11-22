-- just display tables so last requirement is met
-- we need to have all data be accessible some way so this is my way of making sure it is
select teamName, abbrev, city, divisionName from team order by teamName;
select name, age from player order by name;
select * from division order by divisionName;

-- search a player's stats by player name
-- search player fielding stats by player name
select player.name, putouts, assists, errors from fieldingStats
join player on fieldingStats.playerID = player.playerID
where player.name like "%?%";

-- search player pitching stats by player name
select player.name, teamAbbrev, gamesPlayed, hitsAllowed, hrAllowed, walksAllowed, strikeOuts, inningsPitched, earnedRuns from pitchingStats 
join player on pitchingStats.playerID = player.playerID;

-- search player batting stats by player name
select player.name, hrHit, hits, atBats from battingStats
join player on battingStats.playerID = player.playerID
where player.name like "%?%";
--end of searching stat's by player name

-- search a team's stats by team name
-- search team fielding stats by team name
select teamName, totalPutouts, totalAssists,doublePlays from teamFieldingStats
join team on teamFieldingStats.teamID = team.teamID
where teamName like "%?%";

-- search team pitching stats by team name
select teamName, totalIP, totalER, totalStrikeOuts, totalWalksAllowed, totalHRAllowed, totalHitsAllowed from teamPitchingStats
join team on teamPitchingStats.teamID = team.teamID
where teamName like "%?%";

-- search team batting stats by team name
select teamName, totalHR, totalHits, totalAB from teamBattingStats
join team on teamBattingStats.teamID = team.teamID
where teamName like "%?%";
--end of searching stats by team name 

--search a manager's stats by manager name
select managerName, teamName, ejections, challanges, overturned from manager
join team on manager.teamID = team.teamID
where managerName like "%?%";

--search by player name, all teams that player has played on
--ordered by player name so that same players end up beside each other in the result
select name, teamName from playsOn
join player on playsOn.playerID = player.playerID
join team on playsOn.teamID = team.teamID
where name like "%?%"
order by name;

--returns all teams that play in the same city as another team
--order by city so that teams that play in the same city end up beside each other in the result
select teamName, city from team
where city in (
select city from team
group by city
having count(teamName) > 1
)
order by city;

--returns all players that played on multiple teams
--order by name because alphabetical order is nice
select DISTINCT name from player
join playsOn on player.playerID = playsOn.playerID
group by name
having count(teamID) > 1
order by name;

--returns the stats of a divison. Found by grouping the teams by divison, then averaging their stats
--returns fielding stats by division
select divisionName, round(avg(totalPutouts), 2), round(avg(totalAssists), 2), round(avg(totalErrors), 2), round(avg(doublePlays), 2) from team
join teamFieldingStats on team.teamID = teamFieldingStats.teamID
group by divisionName;

--returns pitching stats by division
select divisionName, round(avg(totalIP), 2), round(avg(totalER), 2), round(avg(totalStrikeOuts), 2), round(avg(totalWalksAllowed), 2), round(avg(totalHRAllowed), 2), round(avg(totalHitsAllowed), 2) from team
join teamPitchingStats on team.teamID = teamPitchingStats.teamID
group by divisionName;

--returns batting stats by division
select divisionName, round(avg(totalHR), 2), round(avg(totalHits), 2), round(avg(totalAB), 2) from team
join teamBattingStats on team.teamID = teamBattingStats.teamID
group by divisionName;
--end of stats by division

--returns all players that have both pitching and batting stats (players that have both pitched and batted)
select distinct name from battingStats 
join pitchingStats on pitchingStats.playerID = battingStats.playerID
join player on battingStats.playerID = player.playerID;

--returns all players that only have batting stats (players that have only batted)
select DISTINCT name from battingStats
join player on battingStats.playerID = player.playerID
where name not in (
select DISTINCT name from fieldingStats
join player on fieldingStats.playerID = player.playerID
)
and name not in (
select DISTINCT name from pitchingStats
join player on pitchingStats.playerID = player.playerID
);

--returns the top 10 players by home runs hit
select name, hrHit from battingStats
join player on battingStats.playerID = player.playerID
order by hrHit desc
limit 10;

--returns the top 10 teams by strikeouts per an inning
select team.teamName, round(cast(totalStrikeOuts as float)/cast(totalIP as float), 2) from teamPitchingStats
join team on teamPitchingStats.teamID = team.teamID
order by totalStrikeOuts/totalIP DESC
limit 10;