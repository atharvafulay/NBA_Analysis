select * from season s
join Team t on t.sr_id = p.team_id and t.season_id = s.sr_id
join TeamAverage ta on t.sr_id = ta.team_id and ta.season_id = s.sr_id
join TeamTotal tt on t.sr_id = tt.team_id and tt.season_id = s.sr_id
join player p on s.sr_id = p.season_id and t.season_id = s.sr_id
join playertotal pt on p.sr_id = pt.player_id and pt.team_id = t.sr_id and s.sr_id = pt.season_id
join playeraverage pa on p.sr_id = pa.player_id and pa.team_id = t.sr_id and s.sr_id = pa.season_id
where tt.is_opponent = 0 and ta.is_opponent = 0