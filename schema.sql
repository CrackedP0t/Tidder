--
-- PostgreSQL database dump
--

-- Dumped from database version 11.4
-- Dumped by pg_dump version 11.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: bktree; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS bktree WITH SCHEMA public;


--
-- Name: EXTENSION bktree; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION bktree IS 'BK-tree implementation';


--
-- Name: image_cache_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.image_cache_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: image_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.image_cache (
    id bigint DEFAULT nextval('public.image_cache_id_seq'::regclass) NOT NULL,
    link character varying NOT NULL,
    hash bigint NOT NULL,
    no_store boolean,
    no_cache boolean,
    expires timestamp without time zone,
    etag character varying,
    must_revalidate boolean,
    retrieved_on timestamp without time zone NOT NULL
);


--
-- Name: images; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.images (
    id bigint NOT NULL,
    link character varying NOT NULL,
    hash bigint NOT NULL,
    no_store boolean,
    no_cache boolean,
    expires timestamp without time zone,
    etag character varying,
    must_revalidate boolean,
    retrieved_on timestamp without time zone NOT NULL
);


--
-- Name: images_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.images_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: images_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.images_id_seq OWNED BY public.images.id;


--
-- Name: posts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.posts (
    id bigint NOT NULL,
    reddit_id character varying NOT NULL,
    link character varying NOT NULL,
    permalink character varying NOT NULL,
    author character varying,
    score bigint NOT NULL,
    created_utc timestamp without time zone NOT NULL,
    subreddit character varying NOT NULL,
    title character varying NOT NULL,
    nsfw boolean NOT NULL,
    spoiler boolean DEFAULT false NOT NULL,
    image_id bigint,
    reddit_id_int bigint NOT NULL
);


--
-- Name: posts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.posts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: posts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.posts_id_seq OWNED BY public.posts.id;


--
-- Name: images id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.images ALTER COLUMN id SET DEFAULT nextval('public.images_id_seq'::regclass);


--
-- Name: posts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posts ALTER COLUMN id SET DEFAULT nextval('public.posts_id_seq'::regclass);


--
-- Name: image_cache image_cache_link_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.image_cache
    ADD CONSTRAINT image_cache_link_key UNIQUE (link);


--
-- Name: image_cache image_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.image_cache
    ADD CONSTRAINT image_cache_pkey PRIMARY KEY (id);


--
-- Name: images images_link_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.images
    ADD CONSTRAINT images_link_key UNIQUE (link);


--
-- Name: images images_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.images
    ADD CONSTRAINT images_pkey PRIMARY KEY (id);


--
-- Name: posts posts_permalink_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_permalink_key UNIQUE (permalink);


--
-- Name: posts posts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_pkey PRIMARY KEY (id);


--
-- Name: posts posts_reddit_id_int_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_reddit_id_int_key UNIQUE (reddit_id_int);


--
-- Name: posts posts_reddit_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_reddit_id_key UNIQUE (reddit_id);


--
-- Name: image_cache_hash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX image_cache_hash_idx ON public.image_cache USING spgist (hash public.bktree_ops);


--
-- Name: images_hash_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX images_hash_idx ON public.images USING spgist (hash public.bktree_ops);


--
-- Name: posts_author_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX posts_author_idx ON public.posts USING btree (author);


--
-- Name: posts_image_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX posts_image_id_idx ON public.posts USING btree (image_id);


--
-- Name: posts_subreddit_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX posts_subreddit_idx ON public.posts USING btree (subreddit);


--
-- Name: posts posts_image_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posts
    ADD CONSTRAINT posts_image_id_fkey FOREIGN KEY (image_id) REFERENCES public.images(id);


--
-- PostgreSQL database dump complete
--

