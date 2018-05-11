CREATE KEYSPACE youtube WITH REPLICATION = {
	'class' : 'SimpleStrategy',
    'replication_factor' : 3
};

CREATE TABLE youtube.videos (
  video_id uuid PRIMARY KEY,
  title text,
  description text,
  tags set<text>,
  upload_date date
);