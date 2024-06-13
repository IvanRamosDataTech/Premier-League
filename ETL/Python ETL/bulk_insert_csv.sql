-- Don't forget to first copy your csv data source into container's file system so Postgresql can find it
-- and proceed with bulk inserts. You can perform this by typing following command:

--docker cp dataset/PremierLeague.csv container-pg:/home/PremierLeague.csv

COPY match_history
FROM '/home/PremierLeague.csv'
DELIMITER ','
CSV HEADER;