--
-- PostgreSQL database dump
--

\restrict EXKeLUNGuSGVG2G0TBgxCHMfboDFyBGbGqzJ8XjSHmnsbRr4IEjbnuug1R2zMqx

-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.3 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: category; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.category (
    category_id integer NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.category OWNER TO postgres;

--
-- Name: category_category_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.category_category_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.category_category_id_seq OWNER TO postgres;

--
-- Name: category_category_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.category_category_id_seq OWNED BY public.category.category_id;


--
-- Name: developer; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.developer (
    developer_id integer NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.developer OWNER TO postgres;

--
-- Name: developer_developer_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.developer_developer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.developer_developer_id_seq OWNER TO postgres;

--
-- Name: developer_developer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.developer_developer_id_seq OWNED BY public.developer.developer_id;


--
-- Name: game; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.game (
    appid integer NOT NULL,
    name text NOT NULL,
    developer_id integer,
    publisher_id integer,
    release_date date,
    price numeric(8,2),
    average_playtime integer,
    owners text
);


ALTER TABLE public.game OWNER TO postgres;

--
-- Name: game_category; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.game_category (
    appid integer NOT NULL,
    category_id integer NOT NULL
);


ALTER TABLE public.game_category OWNER TO postgres;

--
-- Name: game_genre; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.game_genre (
    appid integer NOT NULL,
    genre_id integer NOT NULL
);


ALTER TABLE public.game_genre OWNER TO postgres;

--
-- Name: genre; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.genre (
    genre_id integer NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.genre OWNER TO postgres;

--
-- Name: genre_genre_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.genre_genre_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.genre_genre_id_seq OWNER TO postgres;

--
-- Name: genre_genre_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.genre_genre_id_seq OWNED BY public.genre.genre_id;


--
-- Name: platform_support; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.platform_support (
    appid integer NOT NULL,
    windows boolean DEFAULT false NOT NULL,
    mac boolean DEFAULT false NOT NULL,
    linux boolean DEFAULT false NOT NULL
);


ALTER TABLE public.platform_support OWNER TO postgres;

--
-- Name: publisher; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.publisher (
    publisher_id integer NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.publisher OWNER TO postgres;

--
-- Name: publisher_publisher_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.publisher_publisher_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.publisher_publisher_id_seq OWNER TO postgres;

--
-- Name: publisher_publisher_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.publisher_publisher_id_seq OWNED BY public.publisher.publisher_id;


--
-- Name: review; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.review (
    appid integer NOT NULL,
    positive_ratings integer DEFAULT 0 NOT NULL,
    negative_ratings integer DEFAULT 0 NOT NULL
);


ALTER TABLE public.review OWNER TO postgres;

--
-- Name: category category_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.category ALTER COLUMN category_id SET DEFAULT nextval('public.category_category_id_seq'::regclass);


--
-- Name: developer developer_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.developer ALTER COLUMN developer_id SET DEFAULT nextval('public.developer_developer_id_seq'::regclass);


--
-- Name: genre genre_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.genre ALTER COLUMN genre_id SET DEFAULT nextval('public.genre_genre_id_seq'::regclass);


--
-- Name: publisher publisher_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publisher ALTER COLUMN publisher_id SET DEFAULT nextval('public.publisher_publisher_id_seq'::regclass);


--
-- Name: category category_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.category
    ADD CONSTRAINT category_pkey PRIMARY KEY (category_id);


--
-- Name: developer developer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.developer
    ADD CONSTRAINT developer_pkey PRIMARY KEY (developer_id);


--
-- Name: game_category game_category_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_category
    ADD CONSTRAINT game_category_pkey PRIMARY KEY (appid, category_id);


--
-- Name: game_genre game_genre_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_genre
    ADD CONSTRAINT game_genre_pkey PRIMARY KEY (appid, genre_id);


--
-- Name: game game_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game
    ADD CONSTRAINT game_pkey PRIMARY KEY (appid);


--
-- Name: genre genre_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.genre
    ADD CONSTRAINT genre_pkey PRIMARY KEY (genre_id);


--
-- Name: platform_support platform_support_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.platform_support
    ADD CONSTRAINT platform_support_pkey PRIMARY KEY (appid);


--
-- Name: publisher publisher_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publisher
    ADD CONSTRAINT publisher_pkey PRIMARY KEY (publisher_id);


--
-- Name: review review_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.review
    ADD CONSTRAINT review_pkey PRIMARY KEY (appid);


--
-- Name: category uq_category_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.category
    ADD CONSTRAINT uq_category_name UNIQUE (name);


--
-- Name: developer uq_developer_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.developer
    ADD CONSTRAINT uq_developer_name UNIQUE (name);


--
-- Name: genre uq_genre_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.genre
    ADD CONSTRAINT uq_genre_name UNIQUE (name);


--
-- Name: publisher uq_publisher_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.publisher
    ADD CONSTRAINT uq_publisher_name UNIQUE (name);


--
-- Name: game_category game_category_appid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_category
    ADD CONSTRAINT game_category_appid_fkey FOREIGN KEY (appid) REFERENCES public.game(appid) ON DELETE CASCADE;


--
-- Name: game_category game_category_category_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_category
    ADD CONSTRAINT game_category_category_id_fkey FOREIGN KEY (category_id) REFERENCES public.category(category_id) ON DELETE CASCADE;


--
-- Name: game game_developer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game
    ADD CONSTRAINT game_developer_id_fkey FOREIGN KEY (developer_id) REFERENCES public.developer(developer_id);


--
-- Name: game_genre game_genre_appid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_genre
    ADD CONSTRAINT game_genre_appid_fkey FOREIGN KEY (appid) REFERENCES public.game(appid) ON DELETE CASCADE;


--
-- Name: game_genre game_genre_genre_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_genre
    ADD CONSTRAINT game_genre_genre_id_fkey FOREIGN KEY (genre_id) REFERENCES public.genre(genre_id) ON DELETE CASCADE;


--
-- Name: game game_publisher_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game
    ADD CONSTRAINT game_publisher_id_fkey FOREIGN KEY (publisher_id) REFERENCES public.publisher(publisher_id);


--
-- Name: platform_support platform_support_appid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.platform_support
    ADD CONSTRAINT platform_support_appid_fkey FOREIGN KEY (appid) REFERENCES public.game(appid) ON DELETE CASCADE;


--
-- Name: review review_appid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.review
    ADD CONSTRAINT review_appid_fkey FOREIGN KEY (appid) REFERENCES public.game(appid) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict EXKeLUNGuSGVG2G0TBgxCHMfboDFyBGbGqzJ8XjSHmnsbRr4IEjbnuug1R2zMqx

