
CREATE KEYSPACE KillrVideo
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };
  
CREATE TABLE KillrVideo.videos(video_id timeuuid, added_date timestamp, description text, title text, user_id uuid, PRIMARY KEY(video_id));

COPY KillrVideo.videos(video_id, added_date, description, tags, title, user_id) FROM '/usr/local/share/videos.csv' WITH HEADER = TRUE;

TRUNCATE KillrVideo.videos;

CREATE TABLE KillrVideo.videos_by_title_year(title text, added_year int,  added_date timestamp, description text, user_id uuid, video_id timeuuid, PRIMARY KEY((title, added_year),video_id));

COPY KillrVideo.videos_by_title_year(title, added_year, added_date, description, user_id, video_id) FROM '/usr/local/share/videos_by_title_year.csv' WITH HEADER = TRUE;

-- SELECT * FROM killrvideo.videos_by_title_year WHERE added_year=2015;
-- InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
-- Предупреждает, что поиск может быть долгим. Решается через ALLOW FILTERING.

CREATE TABLE KillrVideo.video_by_tag_year(
tag text,
added_year int,
video_id timeuuid,
added_date timestamp,
description text,
title text,
user_id uuid,
PRIMARY KEY((video_id),added_date)
) WITH CLUSTERING ORDER BY(added_date DESC);


COPY KillrVideo.video_by_tag_year(tag, added_year, video_id, added_date, description, title, user_id) FROM '/usr/local/share/videos_by_tag_year.csv' WITH HEADER = TRUE;

ALTER TABLE KillrVideo.videos 
ADD tag text;

COPY KillrVideo.videos(video_id, added_date, description, tag, title, user_id) FROM '/usr/local/share/videos.csv' WITH HEADER = TRUE;

CREATE TYPE video_encoding(
bit_rates SET<TEXT>,
encoding TEXT,
height INT,
width INT,
);

ALTER TABLE KillrVideo.videos 
ADD encoding video_encoding;

COPY KillrVideo.videos(video_id, encoding) FROM '/usr/local/share/videos_encoding.csv' WITH HEADER = TRUE;
