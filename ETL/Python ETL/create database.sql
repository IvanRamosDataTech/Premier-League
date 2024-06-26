CREATE DATABASE premier_league
    WITH
    OWNER = admin
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

COMMENT ON DATABASE premier_league
    IS 'Contains Historical data of English football competition of Premier League era';