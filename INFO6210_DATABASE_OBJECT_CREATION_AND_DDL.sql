--************************************************************************************************************************************************************************************--
-- Database Creation
GO
    USE master;
GO

GO
	DROP DATABASE IF EXISTS sportsanalytics;
GO

GO
    CREATE DATABASE sportsanalytics;

GO

    USE sportsanalytics;

GO 
--************************************************************************************************************************************************************************************--

-- Table Creation

CREATE TABLE Coach (
    CoachID INT PRIMARY KEY,
    [Name] NVARCHAR(255) NOT NULL,
    StartDate DATE NOT NULL,
    CoachingTenure INT NULL
);

CREATE TABLE Franchise (
    FranchiseID INT PRIMARY KEY,
    FranchiseName NVARCHAR(255) NOT NULL,
    FranchiseOwner NVARCHAR(255) NULL
);

CREATE TABLE Tournament (
    TournamentID INT PRIMARY KEY,
    TournamentName VARCHAR(50) NOT NULL,
    StartDate DATE NULL,
    EndDate DATE NULL
);

CREATE TABLE Team (
    TeamID INT PRIMARY KEY,
    TeamName NVARCHAR(255) NOT NULL,
    CoachID INT FOREIGN KEY REFERENCES Coach(CoachID),
    FranchiseID INT NOT NULL FOREIGN KEY REFERENCES Franchise(FranchiseID)
);

CREATE TABLE TeamLine (
    TeamLineID INT PRIMARY KEY,
    TournamentID INT FOREIGN KEY REFERENCES Tournament(TournamentID),
    TeamID INT NOT NULL FOREIGN KEY REFERENCES Team(TeamID)
);

CREATE TABLE Player (
    PlayerID INT PRIMARY KEY,
    [Name] NVARCHAR(255) NOT NULL,
    TeamID INT NULL FOREIGN KEY REFERENCES Team(TeamID),
    JerseyNbr INT NULL,
    TotalMatch INT NULL CHECK (TotalMatch >= 0 ),
    NbrofCatch INT NULL CHECK (NbrofCatch >= 0 ),
    DominantHand VARCHAR(30) NULL CHECK (DominantHand IN ('Right_Hand', 'Left_Hand')),
    BattingAvg FLOAT NULL CHECK (BattingAvg >= 0 ),
    Centuries INT NULL CHECK (Centuries >= 0 ),
    HalfCenturies INT NULL CHECK (HalfCenturies >= 0),
    TotalRuns INT NULL CHECK (TotalRuns >= 0),
    BowlerType VARCHAR(50) NULL CHECK (BowlerType IN ('Left-arm fast','Left-arm fast-medium','Left-arm medium','Left-arm medium-fast','Legbreak','Legbreak googly','Right-arm bowler','Right-arm fast','Right-arm fast-medium',
'Right-arm medium','Right-arm medium-fast','Right-arm offbreak','Slow left-arm chinaman', 'Slow left-arm orthodox')), 
    Wickets INT NULL CHECK (Wickets >= 0)
);


CREATE TABLE Company (
    CompanyID INT PRIMARY KEY,
    CompanyName NVARCHAR(255) NOT NULL,
    IndustryType VARCHAR(100) NULL
);

CREATE TABLE BrandAmbassador (
    AmbassadorID INT PRIMARY KEY,
    CompanyID INT NOT NULL FOREIGN KEY REFERENCES Company(CompanyID),
    PlayerID INT NOT NULL FOREIGN KEY REFERENCES Player(PlayerID),
    StartDate DATE NULL,
    EndDate DATE NULL
);

CREATE TABLE Contract (
    ContractID INT PRIMARY KEY,
    CoachID INT NULL FOREIGN KEY REFERENCES Coach(CoachID),
    PlayerID INT NULL FOREIGN KEY REFERENCES Player(PlayerID),
    TeamID INT NOT NULL FOREIGN KEY REFERENCES Team(TeamID),
    StartDate DATE NOT NULL,
    EndDate DATE NOT NULL,
    Terms VARCHAR(100) NOT NULL
);

CREATE TABLE TeamPerformance (
    TeamID INT PRIMARY KEY FOREIGN KEY REFERENCES Team(TeamID),
    MatchesPlayed INT NULL,
    MatchesWon INT NULL,
    MatchesLost INT NULL,
    TournamentsWon INT NULL
);

CREATE TABLE Venue (
    VenueID INT PRIMARY KEY,
    [Name] NVARCHAR(255) NOT NULL,
    Capacity INT NOT NULL,
    City NVARCHAR(255) NOT NULL,
    [State] NVARCHAR(255) NULL,
    Country NVARCHAR(255) NULL
);


CREATE TABLE [Match] (
    MatchID INT PRIMARY KEY,
    TournamentID INT NOT NULL FOREIGN KEY REFERENCES Tournament(TournamentID),
    VenueID INT NOT NULL FOREIGN KEY REFERENCES Venue(VenueID),
    MatchDate DATE NULL,
    WinningTeamID INT NULL,
    LosingTeamID INT NULL
);

CREATE TABLE MatchStats (
    MatchID INT PRIMARY KEY FOREIGN KEY REFERENCES [Match](MatchID),
    TotalRuns INT NULL,
    TotalWickets INT NULL,
    HighestScore INT NULL,
    BestBowler INT NULL FOREIGN KEY REFERENCES Player(PlayerID),
    BestBatter INT NULL FOREIGN KEY REFERENCES Player(PlayerID),
    Catches INT NULL,
    Runout INT NULL,
    PlayerofMatch INT NULL FOREIGN KEY REFERENCES Player(PlayerID)
);

CREATE TABLE InjuryDetail (
    PhysioID INT PRIMARY KEY,
    PlayerID INT NOT NULL FOREIGN KEY REFERENCES Player(PlayerID),
    InjuryType VARBINARY(555) NULL ,
    InjuryDate VARBINARY(555)  NULL,
    RecoveryDuration VARBINARY(555) NULL
);

CREATE TABLE PlayerLine (
    PlayerID INT FOREIGN KEY REFERENCES Player(PlayerID),
    TournamentID INT FOREIGN KEY REFERENCES Tournament(TournamentID),
    Runs INT NULL,
    Wickets INT NULL,
    Catch INT NULL,
    CONSTRAINT PK_PL PRIMARY KEY (PlayerID, TournamentID)
);

GO

--************************************************************************************************************************************************************************************--

--************************************************************************************************************************************************************************************--
-- Database Creation
-- GO
--     USE master;
-- GO
-- 
-- GO
-- 	DROP DATABASE IF EXISTS sportsanalytics;
-- GO
-- 
-- GO
--     CREATE DATABASE sportsanalytics;

GO

    USE sportsanalytics;

GO 
--************************************************************************************************************************************************************************************--
-- STORED PROCEDURES
GO

--1. RETRIEVE PLAYER INFORMATION

IF OBJECT_ID('GetPlayerInfo', 'P') IS NOT NULL
    DROP PROCEDURE GetPlayerInfo
GO

CREATE PROCEDURE GetPlayerInfo
    @PlayerID INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT p.[Name], t.TeamName, p.TotalRuns, f.FranchiseName, CompanyName
    FROM Player p 
	LEFT JOIN team t on p.TeamID = t.TeamID
	LEFT JOIN Franchise f on t.FranchiseID = f.FranchiseID
	LEFT JOIN BrandAmbassador b on p.PlayerID = b.PlayerID
	LEFT JOIN Company c on c.CompanyID = b.CompanyID
    WHERE P.PlayerID = @PlayerID;
END;
GO


EXEC GetPlayerInfo @PlayerID =1;

GO

--2. TEAM STATISTICS
IF OBJECT_ID('TeamInfo', 'P') IS NOT NULL
    DROP PROCEDURE TeamInfo
GO

CREATE PROCEDURE TeamInfo
    @TeamID INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT t.TeamName, f.FranchiseName, C.[Name] as CoachName, tp.MatchesPlayed, tp.MatchesWon, tp.TournamentsWon, f.FranchiseOwner
    FROM Team t 
	LEFT JOIN TeamPerformance tp on t.TeamID = tp.TeamID
	LEFT JOIN Coach c on t.CoachID = c.CoachID
	LEFT JOIN Franchise f on t.FranchiseID = f.FranchiseID
    WHERE t.TeamID = @TeamID;
END

EXEC TeamInfo @TeamID =1;



--3. STORED PROCEDURE FOR MATCHINFO

IF OBJECT_ID('MatchInfo', 'P') IS NOT NULL
    DROP PROCEDURE MatchInfo
GO


CREATE PROCEDURE MatchInfo
    @MatchID INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT m.MatchID, t.TeamName as WinningTeam, v.[Name] as VenueName, ms.PlayerofMatch
    FROM [Match] m 
	LEFT JOIN Venue v on m.VenueID = v.VenueID
	LEFT JOIN Team t on t.TeamID = m.WinningTeamID
	LEFT JOIN MatchStats ms on m.MatchID = ms.MatchID
    WHERE m.MatchID = @MatchID;
END

EXEC MatchInfo @MatchID =1;


--4. STORED PROCEDURE FOR WinningRate of a Team

IF OBJECT_ID('GetTeamWinningRate', 'P') IS NOT NULL
    DROP PROCEDURE GetTeamWinningRate
GO


CREATE PROCEDURE GetTeamWinningRate
    @TeamID INT,
    @WinningRate FLOAT OUTPUT
AS
BEGIN

    SET @WinningRate = 0;
    
    IF EXISTS (SELECT 1 FROM TeamPerformance WHERE TeamID = @TeamID)
    BEGIN
        SELECT 
            @WinningRate = ISNULL(CAST(MatchesWon AS FLOAT) / NULLIF(MatchesPlayed, 0), 0)
        FROM 
            TeamPerformance
        WHERE 
            TeamID = @TeamID;
    END
END;
GO

-- Query to Display the WinningRate of a Team
DECLARE @WinRate FLOAT;
EXEC GetTeamWinningRate @TeamID = 1, @WinningRate = @WinRate OUTPUT;

SELECT @WinRate AS WinningRate;


--5. STORED PROCEDURE For Find the number of Matches played at a Venue

IF OBJECT_ID('GetMatchesCountAtVenue', 'P') IS NOT NULL
    DROP PROCEDURE GetMatchesCountAtVenue
GO


CREATE PROCEDURE GetMatchesCountAtVenue
    @VenueID INT,
    @MatchesCount INT OUTPUT
AS
BEGIN
    SELECT @MatchesCount = COUNT(*)
    FROM [Match]
    WHERE VenueID = @VenueID;
END;
GO

-- Query to Display the number of Matches played at a Venue

DECLARE @TotalMatches INT;
EXEC GetMatchesCountAtVenue @VenueID = 1, @MatchesCount = @TotalMatches OUTPUT; 

SELECT @TotalMatches AS TotalMatchesPlayed;


--6. STORED PROCEDURE For Find the number of Matches played by a Team in a Tournament

IF OBJECT_ID('GetMatchesCountInTournamentByTeam', 'P') IS NOT NULL
    DROP PROCEDURE GetMatchesCountInTournamentByTeam
GO


CREATE PROCEDURE GetMatchesCountInTournamentByTeam
    @TournamentID INT,
    @TeamID INT,
    @MatchesCount INT OUTPUT
AS
BEGIN
    SELECT @MatchesCount = COUNT(*)
    FROM [Match]
    WHERE TournamentID = @TournamentID
    AND (WinningTeamID = @TeamID OR LosingTeamID = @TeamID);
END;
GO

-- Query to Display the number of Matches played by a Team in a Tournament


DECLARE @TotalMatches INT;
EXEC GetMatchesCountInTournamentByTeam 
    @TournamentID = 1,
    @TeamID = 1,  
    @MatchesCount = @TotalMatches OUTPUT;

SELECT @TotalMatches AS TotalMatchesPlayed;


--6. STORED PROCEDURE For Find the number of Matches played by a Team in a Tournament at a Venue

IF OBJECT_ID('GetMatchCountByTeamTournamentVenue', 'P') IS NOT NULL
    DROP PROCEDURE GetMatchCountByTeamTournamentVenue
GO


CREATE PROCEDURE GetMatchCountByTeamTournamentVenue
    @TeamID INT,
    @TournamentID INT,
    @VenueID INT,
    @MatchCount INT OUTPUT
AS
BEGIN
    SELECT @MatchCount = COUNT(*)
    FROM [Match]
    WHERE TournamentID = @TournamentID
      AND VenueID = @VenueID
      AND (WinningTeamID = @TeamID OR LosingTeamID = @TeamID);
END;
GO


-- Query to Display the number of Matches played by a Team in a Tournament at a Venue


DECLARE @NumberOfMatches INT;
EXEC GetMatchCountByTeamTournamentVenue 
    @TeamID = 1,    
    @TournamentID = 1, 
    @VenueID = 1,      
    @MatchCount = @NumberOfMatches OUTPUT;

SELECT @NumberOfMatches AS TotalMatchesPlayed;




--************************************************************************************************************************************************************************************--

-- AUDIT TABLE AND TRIGGERS

CREATE TABLE [dbo].[SportsAudit] (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128),
    ID INT,
    [Action] CHAR(1),
    ActionDate DATETIME,
    OldValues NVARCHAR(MAX),
    NewValues NVARCHAR(MAX),
    UpdatedBy NVARCHAR(128)
);

GO
CREATE TRIGGER trg_Coach_Audit
ON Coach
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Handle inserted rows
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action],ActionDate, NewValues, UpdatedBy)
        SELECT 'Coach', i.CoachID, 'I', GETDATE(), CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), SUSER_NAME()
        FROM inserted i;
    END

    -- Handle updated rows
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action], ActionDate, OldValues, NewValues, UpdatedBy)
        SELECT 'Coach', i.CoachID, 'U', GETDATE(), 
            CONVERT(NVARCHAR(MAX), (SELECT d.* FOR JSON PATH)), 
            CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), 
            SUSER_NAME()
        FROM inserted i
        INNER JOIN deleted d ON i.CoachID = d.CoachID;
    END
END;
GO

GO
CREATE TRIGGER trg_Franchise_Audit
ON Franchise
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Handle inserted rows
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action],ActionDate, NewValues, UpdatedBy)
        SELECT 'Franchise', i.FranchiseID, 'I', GETDATE(), CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), SUSER_NAME()
        FROM inserted i;
    END

    -- Handle updated rows
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action], ActionDate, OldValues, NewValues, UpdatedBy)
        SELECT 'Franchise', i.FranchiseID, 'U', GETDATE(), 
            CONVERT(NVARCHAR(MAX), (SELECT d.* FOR JSON PATH)), 
            CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), 
            SUSER_NAME()
        FROM inserted i
        INNER JOIN deleted d ON i.FranchiseID = d.FranchiseID;
    END
END;
GO


GO
CREATE TRIGGER trg_Team_Audit
ON Team
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Handle inserted rows
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action],ActionDate, NewValues, UpdatedBy)
        SELECT 'Team', i.TeamID, 'I', GETDATE(), CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), SUSER_NAME()
        FROM inserted i;
    END

    -- Handle updated rows
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action], ActionDate, OldValues, NewValues, UpdatedBy)
        SELECT 'Team', i.TeamID, 'U', GETDATE(), 
            CONVERT(NVARCHAR(MAX), (SELECT d.* FOR JSON PATH)), 
            CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), 
            SUSER_NAME()
        FROM inserted i
        INNER JOIN deleted d ON i.TeamID = d.TeamID;
    END
END;
GO

GO
CREATE TRIGGER trg_Player_Audit
ON Player
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Handle inserted rows
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action],ActionDate, NewValues, UpdatedBy)
        SELECT 'Player', i.PlayerID, 'I', GETDATE(), CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), SUSER_NAME()
        FROM inserted i;
    END

    -- Handle updated rows
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action], ActionDate, OldValues, NewValues, UpdatedBy)
        SELECT 'Player', i.PlayerID, 'U', GETDATE(), 
            CONVERT(NVARCHAR(MAX), (SELECT d.* FOR JSON PATH)), 
            CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), 
            SUSER_NAME()
        FROM inserted i
        INNER JOIN deleted d ON i.PlayerID = d.PlayerID;
    END
END;
GO


GO
CREATE TRIGGER trg_Company_Audit
ON Company
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Handle inserted rows
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action],ActionDate, NewValues, UpdatedBy)
        SELECT 'Company', i.CompanyID, 'I', GETDATE(), CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), SUSER_NAME()
        FROM inserted i;
    END

    -- Handle updated rows
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO [dbo].[SportsAudit] (TableName, ID, [Action], ActionDate, OldValues, NewValues, UpdatedBy)
        SELECT 'Company', i.CompanyID, 'U', GETDATE(), 
            CONVERT(NVARCHAR(MAX), (SELECT d.* FOR JSON PATH)), 
            CONVERT(NVARCHAR(MAX), (SELECT i.* FOR JSON PATH)), 
            SUSER_NAME()
        FROM inserted i
        INNER JOIN deleted d ON i.CompanyID = d.CompanyID;
    END
END;
GO








--************************************************************************************************************************************************************************--


CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'DAMG6210!';

CREATE CERTIFICATE DAMGGROUP12Certificate
WITH SUBJECT = 'Injury Details Encryption';

CREATE SYMMETRIC KEY DAMGGROUP12SymmetricKey
WITH ALGORITHM = AES_256
ENCRYPTION BY CERTIFICATE DAMGGROUP12Certificate;



/*
OPEN SYMMETRIC KEY DAMGGROUP12SymmetricKey
DECRYPTION BY CERTIFICATE DAMGGROUP12Certificate;

-- USED WHILE INSERTING DATA INTO THE InjuryDetail TABLE

CLOSE SYMMETRIC KEY DAMGGROUP12SymmetricKey;
*/


OPEN SYMMETRIC KEY DAMGGROUP12SymmetricKey
DECRYPTION BY CERTIFICATE DAMGGROUP12Certificate;

SELECT PhysioID, PlayerID,
    CONVERT(VARCHAR, DecryptByKey(InjuryType)) AS InjuryType,
    CONVERT(VARCHAR, DecryptByKey(InjuryDate)) AS InjuryDate,
    CONVERT(VARCHAR, DecryptByKey(RecoveryDuration)) AS RecoveryDuration
FROM InjuryDetail;

CLOSE SYMMETRIC KEY DAMGGROUP12SymmetricKey;


--*****************************************************************************************************************************************************************************--

-- USER DEFINED FUNCTION FOR CALCULATING THE TENURE OF A COACH
GO
CREATE OR ALTER FUNCTION dbo.CalculateTenure(@StartDate DATE)
RETURNS INT
AS
BEGIN
    RETURN DATEDIFF(YEAR, @StartDate, GETDATE());
END;
GO


-- USER DEFINED FUNCTION FOR CALCULATING THE BATTING AVERAGE OF A BATSMAN
GO
CREATE OR ALTER FUNCTION dbo.CalculateBattingAverage
(
    @TotalRuns INT,
    @TotalMatch INT
)
RETURNS INT
AS
BEGIN
    DECLARE @BattingAverage FLOAT;

    SELECT @BattingAverage = CAST(COALESCE(@TotalRuns, 0.0) AS FLOAT) / COALESCE(@TotalMatch,1);


    RETURN CAST (@BattingAverage AS INT);
END;
GO


--*****************************************************************************************************************************************************************************--
-- NON - CLUSTERED INDEX
GO
-- Create a non-clustered index on TeamID in Player
CREATE NONCLUSTERED INDEX IX_Player_TeamID ON Player (TeamID);
GO

-- Create a composite non-clustered index on TeamID and BattingAvg in Player
CREATE NONCLUSTERED INDEX IX_Player_TeamID_BattingAvg ON Player (TeamID, BattingAvg);
GO


-- Create a non-clustered index on MatchesWon in TeamPerformance
CREATE NONCLUSTERED INDEX IX_TeamPerformance_MatchesWon ON TeamPerformance (MatchesWon);
GO

-- Create a composite non-clustered index on MatchesPlayed and MatchesWon in TeamPerformance
CREATE NONCLUSTERED INDEX IX_TeamPerformance_MatchesPlayed_MatchesWon ON TeamPerformance (MatchesPlayed, MatchesWon);
GO


-- Create a non-clustered index on VenueID in [Match]
CREATE NONCLUSTERED INDEX IX_Match_VenueID ON [Match] (VenueID);
GO

-- Create a composite non-clustered index on WinningTeamID and LosingTeamID in [Match]
CREATE NONCLUSTERED INDEX IX_Match_WinningTeamID_LosingTeamID ON [Match] (WinningTeamID, LosingTeamID);
GO

-- Query to check indexes created ON Table : 'Player'
SELECT NAME      AS index_name,
       type_desc AS index_type
FROM   sys.indexes
WHERE  object_id = Object_id('Player');
GO

-- Query to check indexes created ON Table : 'TeamPerformance'
SELECT NAME      AS index_name,
       type_desc AS index_type
FROM   sys.indexes
WHERE  object_id = Object_id('TeamPerformance');
GO

-- Query to check indexes created ON Table : 'Match'
SELECT NAME      AS index_name,
       type_desc AS index_type
FROM   sys.indexes
WHERE  object_id = Object_id('Match');
GO


--*****************************************************************************************************************************************************************************--
-- VIEWS
GO

-- High Level Summary of a TEAM

CREATE OR ALTER VIEW TeamSummary AS

SELECT t.TeamName, f.FranchiseName, C.[Name] as CoachName, tp.MatchesPlayed, tp.MatchesWon, tp.TournamentsWon, f.FranchiseOwner
    FROM Team t 
	LEFT JOIN TeamPerformance tp on t.TeamID = tp.TeamID
	LEFT JOIN Coach c on t.CoachID = c.CoachID
	LEFT JOIN Franchise f on t.FranchiseID = f.FranchiseID

GO


-- Summary of Matches played by team at a Venue in a Tournament

CREATE OR ALTER VIEW TeamSummaryByTournamentVenue AS
SELECT
    tm.TeamName,
    v.Name as VenueName,
    t.TournamentName,
    COUNT(m.MatchID) AS TotalMatches,
    SUM(CASE WHEN m.WinningTeamID = tm.TeamID THEN 1 ELSE 0 END) AS Wins,
    SUM(CASE WHEN m.LosingTeamID = tm.TeamID THEN 1 ELSE 0 END) AS Losses
FROM 
    [Match] m
INNER JOIN Venue v ON m.VenueID = v.VenueID
INNER JOIN Tournament t ON m.TournamentID = t.TournamentID
LEFT JOIN Team tm ON m.WinningTeamID = tm.TeamID OR m.LosingTeamID = tm.TeamID
GROUP BY 
    v.Name, 
    t.TournamentName, 
    tm.TeamName, 
    tm.TeamID

GO

-- High Level Summary of a Player

CREATE OR ALTER VIEW PlayerSummary AS
SELECT p.[Name] PlayerName, t.TeamName, p.TotalRuns,p.wickets, f.FranchiseName, c.CompanyName, ch.[Name] as CoachName
    FROM Player p 
	LEFT JOIN team t on p.TeamID = t.TeamID
	LEFT JOIN Franchise f on t.FranchiseID = f.FranchiseID
	LEFT JOIN BrandAmbassador b on p.PlayerID = b.PlayerID
	LEFT JOIN Company c on c.CompanyID = b.CompanyID
	LEFT JOIN Coach ch on t.CoachID = ch.CoachID
	
GO	
	

-- Frequency of Matches by country
CREATE VIEW MatchVenueDistribution AS
SELECT
    V.Country,
    COUNT(M.MatchID) AS TotalMatches
FROM
    Venue V
LEFT JOIN
    [Match] M ON V.VenueID = M.VenueID
GROUP BY
    V.Country;


GO

-- Player statistics by each season
CREATE VIEW PlayerStatsView AS
SELECT 
    pl.PlayerID,
    p.Name AS PlayerName,
    t.TeamName,
    pl.TournamentID,
	YEAR(trn.StartDate) AS TournamentYear,
    trn.TournamentName,
    pl.Runs,
    pl.Wickets,
    pl.[Catch],
    p.Centuries,
    p.HalfCenturies
FROM 
    PlayerLine pl
inner JOIN 
    Player p ON pl.PlayerID = p.PlayerID
inner JOIN
    Team t ON p.TeamID = t.TeamID
LEFT JOIN
    Tournament trn ON pl.TournamentID = trn.TournamentID;

GO

-- Season Statistics 
CREATE VIEW SeasonStatsView AS
select  t.TournamentName, 
	SUM(ms.Totalwickets) AS SeasonWickets,
	SUM(ms.Catches) AS SeasonCatches, 
	AVG(ms.totalRuns) As SeasonRuns
	from matchstats ms
join match m on m.MatchID = ms.MatchID
join tournament t on m.TournamentID = t.tournamentID
group by t.TournamentName

GO


-- Thorough Season Analysis with every match detail
Create view SeasonAnalysis AS
SELECT 
    ms.MatchID, 
    t.TournamentName as Season, 
    v.[Name] as MatchStadium,
    m.MatchDate,
	tm.TeamName as WinningTeam,
	tl.TeamName as LosingTeam,
    pb.[Name] AS BestBowler, 
    bb.[Name] AS BestBatsman, 
    pm.[Name] AS PlayerOfMatch, 
    ms.Totalwickets, 
    ms.Catches, 
    ms.totalRuns 
FROM 
    matchstats ms
JOIN 
    match m ON m.MatchID = ms.MatchID
LEFT JOIN
	tournament t on m.TournamentID = t.tournamentID
LEFT JOIN 
	Team tm on tm.TeamID = m.WinningTeamID
LEFT JOIN 
	Team tl on tl.TeamID = m.LosingTeamID
LEFT JOIN
	Venue v on  m.VenueID=  v.VenueID
LEFT JOIN 
    player pb ON ms.BestBowler = pb.PlayerID  
LEFT JOIN  
    player bb ON ms.BestBatter = bb.PlayerID  -- matches won based on chosen team
LEFT JOIN 
    player pm ON ms.PlayerofMatch = pm.PlayerID


GO

-- Team wise Analysis 
Create View TeamAnalysis as 
SELECT t.TeamID, sa.TeamName, sa.Season, tp.TournamentsWon,
       SUM(sa.TotalWickets) AS TotalWickets,
       SUM(sa.TotalRuns) AS TotalRuns,
       SUM(sa.Catches) AS TotalCatches,
       SUM(sa.Wins) AS Wins
FROM (
    SELECT DISTINCT WinningTeam AS TeamName, Season,
                    totalWickets, totalRuns, Catches, 1 as Wins
    FROM SeasonAnalysis
    UNION ALL
    SELECT DISTINCT LosingTeam AS TeamName, Season,
                    totalWickets, totalRuns, Catches, 0 as Wins
    FROM SeasonAnalysis
) AS sa
LEFT JOIN team t ON t.TeamName = sa.TeamName
LEFT JOIN teamperformance tp on tp.TeamID = t.TeamID
GROUP BY sa.Season, sa.TeamName, t.TeamID, tp.TournamentsWon
HAVING t.TeamID IS NOT NULL;

GO

	
--*****************************************************************************************************************************************************************************--
