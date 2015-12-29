CREATE SCHEMA music;

CREATE TABLE music.artist (
  artist_id int NOT NULL,
  artist_name char(45) NOT NULL,
  CONSTRAINT artist_pk PRIMARY KEY (artist_id));

CREATE TABLE music.album (
  album_id int NOT NULL,
  album_name char(45) NOT NULL,
  primary_artist_id int DEFAULT NULL,
  publish_date date NOT NULL,
  CONSTRAINT album_pk PRIMARY KEY (album_id),
  CONSTRAINT album_primary_artist_fk FOREIGN KEY (primary_artist_id) REFERENCES music.artist (artist_id) ON DELETE NO ACTION ON UPDATE NO ACTION);

CREATE TABLE music.copyright_owner (
  owner_id int NOT NULL,
  owner_name char(80) NOT NULL,
  CONSTRAINT copyright_owner_pk PRIMARY KEY (owner_id) );

CREATE TABLE music.song (
  song_id int NOT NULL,
  artist_id int NOT NULL,
  song_name char(45) NOT NULL,
  version char(45) DEFAULT NULL,
  copyright_owner_id int NOT NULL,
  rating int,
  CONSTRAINT song_pk PRIMARY KEY (song_id),
  CONSTRAINT song_copyright_owner_fk FOREIGN KEY (copyright_owner_id) REFERENCES music.copyright_owner (owner_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT song_artist_fk FOREIGN KEY (artist_id) REFERENCES music.artist (artist_id) ON DELETE NO ACTION ON UPDATE NO ACTION);

CREATE TABLE music.copyright (
  copyright_id int NOT NULL,
  song_id int NOT NULL,
  owner_id int NOT NULL,
  copyright_fee int,
  copyright_notes varchar(80) DEFAULT NULL,
  PRIMARY KEY (copyright_id),
  CONSTRAINT copyright_owner_fk FOREIGN KEY (owner_id) REFERENCES music.copyright_owner (owner_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT copyright_song_fk FOREIGN KEY (song_id) REFERENCES music.song (song_id) ON DELETE NO ACTION ON UPDATE NO ACTION);

-- consciously avoid an int key on genre, to force tougher string joins...
CREATE TABLE music.genre (
  genre_name char(40) NOT NULL,
  CONSTRAINT genre_uq UNIQUE (genre_name)
);

-- consciously avoid an int key on tags, to force tougher string joins...
CREATE TABLE music.tag (
  tag_name char(40),
  CONSTRAINT tag_uq UNIQUE (tag_name)
);


CREATE TABLE music.tracks (
  album_id int NOT NULL,
  disk_number int NOT NULL,
  track_number int NOT NULL,
  duration_secs int NOT NULL,
  song_id int DEFAULT NULL,
  genre_name char(40) NOT NULL,
  CONSTRAINT tracks_pk PRIMARY KEY (album_id,disk_number,track_number),
  CONSTRAINT tracks_song_fk FOREIGN KEY (song_id) REFERENCES music.song (song_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT tracks_album_fk FOREIGN KEY (album_id) REFERENCES music.album (album_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT tracks_genre_fk FOREIGN KEY (genre_name) REFERENCES music.genre (genre_name) ON DELETE NO ACTION ON UPDATE NO ACTION
);

CREATE TABLE music.track_tags (
  album_id int NOT NULL,
  disk_number int NOT NULL,
  track_number int NOT NULL,
  tag_name char(40) NOT NULL,
  CONSTRAINT track_tags_tag_fk FOREIGN KEY (tag_name) REFERENCES music.tag (tag_name) ON DELETE NO ACTION ON UPDATE NO ACTION
);

