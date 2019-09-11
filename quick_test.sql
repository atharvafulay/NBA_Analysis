BEGIN TRANSACTION;
SELECT * FROM Player;

UPDATE Player
	SET jersey_number = NULL
	WHERE jersey_number = 'None';

SELECT * FROM Player;
ROLLBACK;



SELECT * FROM Season s
JOIN Team t ON t.season_id = s.sr_id
JOIN TeamTotal tt ON tt.team_id = t.sr_id
JOIN TeamAverage ta ON ta.team_id = t.sr_id
JOIN Player p ON p.team_id = t.sr_id
JOIN PlayerTotal pt ON pt.player_id = p.sr_id
JOIN PlayerAverage pa ON pa.player_id = p.sr_id
WHERE tt.is_opponent = 0 AND ta.is_opponent = 0
AND p.first_name = 'Luol';
