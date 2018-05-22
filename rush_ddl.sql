CREATE EXTERNAL TABLE rush_hive
(
	RANK INT
  	,PLAYER_NAME VARCHAR(30)
  	,TEAM VARCHAR(5)
  	,AGE INT
  	,POS VARCHAR(5)
  	,GAMES_PLAYED INT
  	,GAMES_STARTED INT
  	,RUSHING_ATTEMPTS INT
  	,RUSHING_YARDS INT
  	,RUSHING_TD INT
  	,LONGEST_RUSH INT
  	,RUSH_YARDS_PER_ATTEMPT FLOAT
  	,RUSH_YARDS_PER_GAME FLOAT
  	,RUSHING_ATTEMPTS_PER_GAME FLOAT
  	,REC_TARGET INT
  	,REC_RECEPTIONS INT
  	,REC_YARDS INT
  	,REC_YARDS_PER_RECEPTION FLOAT
  	,REC_TD INT
  	,LONGEST_REC INT
  	,REC_PER_GAME FLOAT
  	,REC_YARDS_PER_GAME FLOAT
  	,CATCH_PERCENTAGE VARCHAR(5)
  	,YARDS_FROM_SCRIMMAGE INT
  	,TOTAL_TD INT
  	,FMBLS INT
  	,YEAR INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'wasbs:///rush-hive.csv'
TBLPROPERTIES("skip.header.line.count"="1");