-- Exercise 1 - Create a keyspace
CREATE KEYSPACE youtube WITH REPLICATION = {
	'class' : 'SimpleStrategy',
    'replication_factor' : 3
};


-- Exercise 2 - Create the 'videos' table
CREATE TABLE youtube.videos (
  video_id uuid PRIMARY KEY,
  title text,
  description text,
  tags set<text>,
  uploaded_by uuid,
  upload_date date
);


-- Exercise 3 - Create the 'users' table
CREATE TABLE youtube.users (
  user_id uuid,
  username text PRIMARY KEY,
  email_address text,
  create_date date,
  first_name text,
  last_name text,
  country text
);


-- Exercise 4 - Create a User Defined Type
CREATE TYPE youtube.user_opinion (
  user_id uuid,
  opinion int
);


-- Exercise 5 - Alter the videos table
ALTER TABLE youtube.videos
ADD likes set<frozen<user_opinion>>;

-- Exercise 6 - Create an email partitioned user table
CREATE TABLE youtube.users_by_email_address (
    user_id uuid,
    username text,
    email_address text PRIMARY KEY,
    create_date date,
    first_name text,
    last_name text,
    country text
);


-- Exercise 7 - Create the title partitioned videos table
CREATE TABLE youtube.videos_by_title (
    video_id uuid,
    title text,
    description text,
    tags set<text>,
    likes set<frozen<user_opinion>>,
    upload_date date,
    uploaded_by uuid,
    PRIMARY KEY ((title), video_id)
);


-- Exercise 8 - Create the user id partitioned videos table
CREATE TABLE youtube.videos_by_user_by_date (
    video_id uuid,
    title text,
    description text,
    tags set<text>,
    likes set<frozen<user_opinion>>,
    upload_date date,
    uploaded_by uuid,
    PRIMARY KEY ((uploaded_by), upload_date)
)
WITH CLUSTERING ORDER BY (upload_date DESC);


-- Exercise 9 - Create the video id partitioned comments table clustered by comment date (Descending)
CREATE TABLE youtube.comments_by_video_by_date (
    comment_id timeuuid,
    video_id uuid,
    user_name text,
    user_id uuid,
    comment text,
    commented_by uuid,
    comment_date date,
    likes set<frozen<user_opinion>>,
    PRIMARY KEY ((video_id), comment_date, comment_id)
)
WITH CLUSTERING ORDER BY (comment_date DESC);


-- Exercise 10 - Create the user id partitioned comments table clustered by comment date (Descending)
CREATE TABLE youtube.comments_by_user_by_date (
    comment_id timeuuid,
    video_id uuid,
    user_name text,
    user_id uuid,
    comment text,
    commented_by uuid,
    comment_date date,
    likes set<frozen<user_opinion>>,
    PRIMARY KEY ((commented_by), comment_date, comment_id)
)
WITH CLUSTERING ORDER BY (comment_date DESC);

