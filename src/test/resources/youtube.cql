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
CREATE TYPE youtube.user_opinion (
  user_id uuid,
  opinion int
);

-- Exercise 6 - Add UDT to videos
ALTER TABLE youtube.videos
ADD likes set<frozen<user_opinion>>;


-- Exercise 7 - Create yet another table
CREATE TABLE youtube.comments (
    comment_id timeuuid PRIMARY KEY,
    video_id uuid,
    user_name text,
    user_id uuid,
    comment text,
    commented_by uuid,
    comment_date date,
    likes set<frozen<user_opinion>>
);

-- Exercise 8 - Use clustering
CREATE TABLE youtube.videos_by_user (
    video_id uuid,
    title text,
    description text,
    tags set<text>,
    likes set<frozen<user_opinion>>,
    upload_date date,
    uploaded_by uuid,
    PRIMARY KEY ((uploaded_by), video_id)
);

-- Exercise 9 - More complex clustering
CREATE TABLE youtube.videos_by_user_by_date (
    video_id uuid,
    title text,
    description text,
    tags set<text>,
    likes set<frozen<user_opinion>>,
    upload_date date,
    uploaded_by uuid,
    PRIMARY KEY ((uploaded_by), upload_date, video_id))
    WITH CLUSERING ORDER BY (upload_date DESC);

-- Exercise 10 - Even More clustering
CREATE TABLE youtube.comments_by_video (
    comment_id timeuuid PRIMARY KEY,
    video_id uuid,
    user_name text,
    user_id uuid,
    comment text,
    commented_by uuid,
    comment_date date,
    likes set<frozen<user_opinion>>,
    PRIMARY KEY ((video_id), commented_by))
    WITH CLUSERING ORDER BY (upload_date DESC);
);
