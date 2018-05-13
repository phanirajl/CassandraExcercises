-- Exercise 1 - Create a keyspace
CREATE KEYSPACE youtube WITH REPLICATION = {
	'class' : 'SimpleStrategy',
    'replication_factor' : 3
};


-- Exercise 2 - Create a table
CREATE TABLE youtube.videos (
  video_id uuid PRIMARY KEY,
  title text,
  description text,
  tags set<text>,
  upload_date date
);

-- Exercise 3 - Create another table
CREATE TABLE youtube.users (
  user_id uuid,
  username text PRIMARY KEY,
  create_date date,
  firstname text,
  lastname text,
  country text
);

-- Exercise 4 - Add user id to videos
ALTER TABLE youtube.videos
ADD uploaded_by uuid;

-- Exercise 5 - Create a UDT