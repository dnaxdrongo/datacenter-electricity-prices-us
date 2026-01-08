CREATE ROLE dc_capstone_user LOGIN PASSWORD 'Munchkin0820$!';
CREATE DATABASE dc_capstone OWNER dc_capstone_user;

\c dc_capstone

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS mart;
