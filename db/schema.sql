-- Steam Explorer Schema
-- CS 457 Final Project
-- Aidan Swan

DROP TABLE IF EXISTS game_category CASCADE;
DROP TABLE IF EXISTS game_genre CASCADE;
DROP TABLE IF EXISTS platform_support CASCADE;
DROP TABLE IF EXISTS review CASCADE;
DROP TABLE IF EXISTS game CASCADE;
DROP TABLE IF EXISTS category CASCADE;
DROP TABLE IF EXISTS genre CASCADE;
DROP TABLE IF EXISTS developer CASCADE;
DROP TABLE IF EXISTS publisher CASCADE;

-- Lookup Tables

CREATE TABLE publisher (
    publisher_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_publisher_name UNIQUE (name)
);

CREATE TABLE developer(
    developer_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_developer_name UNIQUE (name)
);

CREATE TABLE genre (
    genre_id  SERIAL  PRIMARY KEY,
    name      TEXT    NOT NULL,
    CONSTRAINT uq_genre_name UNIQUE (name)
);

CREATE TABLE category(
    category_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_category_name UNIQUE (name)
);

-- Core Tables

CREATE TABLE game(
    appid INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    developer_id INTEGER REFERENCES developer(developer_id),
    publisher_id INTEGER REFERENCES publisher(publisher_id),
    release_date DATE,
    price NUMERIC(8,2),
    average_playtime INTEGER,
    owners TEXT
);

-- Dependent Tables

CREATE TABLE review(
    appid INTEGER PRIMARY KEY REFERENCES game(appid) ON DELETE CASCADE,
    positive_ratings INTEGER NOT NULL DEFAULT 0,
    negative_ratings INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE platform_support(
    appid INTEGER PRIMARY KEY REFERENCES game(appid) ON DELETE CASCADE,
    windows BOOLEAN NOT NULL DEFAULT FALSE,
    mac BOOLEAN NOT NULL DEFAULT FALSE,
    linux BOOLEAN NOT NULL DEFAULT FALSE
);

-- Junction Tables

CREATE TABLE game_genre(
    appid INTEGER REFERENCES game(appid) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genre(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, genre_id)    
);

CREATE TABLE game_category(
    appid INTEGER REFERENCES game(appid) ON DELETE CASCADE,
    category_id INTEGER REFERENCES category(category_id) ON DELETE CASCADE,
    PRIMARY KEY (appid, category_id)
);