--Data scraped from : 
--https://www.baseball-reference.com/leagues/majors/2022.shtml
--https://www.baseball-reference.com/leagues/majors/2022-standard-pitching.shtml
--https://www.baseball-reference.com/leagues/majors/2022-standard-batting.shtml
--https://www.baseball-reference.com/leagues/majors/2022-standard-fielding.shtml
--https://www.baseball-reference.com/leagues/majors/2022-managers.shtml


-- clean up
drop table if exists manager;
drop table if exists teamFieldingStats;
drop table if exists teamBattingStats;
drop table if exists teamPitchingStats;
drop table if exists fieldingStats;
drop table if exists battingStats;
drop table if exists pitchingStats;
drop table if exists playsOn;
drop table if exists player;
drop table if exists team;
drop table if exists division;

create table division (
    divisionName varchar(50) primary key,
    abbreviation text
);

create table team (
    teamName varchar(100) primary key,
    city text not null,
    divisionName varchar(50) references division(divisionName)
);

create table player (
    playerID integer primary key IDENTITY(1,1),
    age integer not null,
    name text not null
);

create table playsOn (
    playerID integer references player(playerID),
    teamName varchar(100) references team(teamName),
    primary key (playerID, teamName)
);

create table pitchingStats (
    pitchingStatsID integer primary key IDENTITY(1,1),
    teamName text not null,
    gamesPlayed integer not null,
    hitsAllowed integer not null,
    hrAllowed integer not null,
    walksAllowed integer not null,
    strikeOuts integer not null,
    inningsPitched integer not null,
    earnedRuns integer not null,
    playerID integer references player(playerID)
);

create table battingStats (
    battingStatsID integer primary key IDENTITY(1,1),
    hrHit integer not null,
    hits integer not null,
    atBats integer not null,
    playerID integer references player(playerID)
);

create table fieldingStats (
    fieldingStatsID integer primary key IDENTITY(1,1),
    putouts integer not null,
    assists integer not null,
    errors integer not null,
    playerID integer references player(playerID)
);

create table teamPitchingStats (
    tpid integer primary key IDENTITY(1,1),
    totalIP float not null,
    totalER integer not null,
    totalStrikeOuts integer not null,
    totalWalksAllowed integer not null,
    totalHRAllowed integer not null,
    totalHitsAllowed integer not null,
    teamName varchar(100) references team(teamName)
);

create table teamBattingStats (
    TBID integer primary key IDENTITY(1,1),
    totalHR integer not null,
    totalHits integer not null,
    totalAB integer not null,
    teamName varchar(100) references team(teamName)
);

create table teamFieldingStats (
    TFID integer primary key IDENTITY(1,1),
    totalPutouts integer not null,
    totalAssists integer not null,
    totalErrors integer not null,
    doublePlays integer not null,
    teamName varchar(100) references team(teamName)
);

create table manager (
    managerName varchar(100) primary key,
    ejections integer not null,
    challanges integer not null,
    overturned integer not null,
    teamName varchar(100) references team(teamName)
);