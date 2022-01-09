-- PRAGMA journal_mode = WAL;
-- PRAGMA synchronous = OFF;

-- PRAGMA journal_mode = OFF;
-- PRAGMA foreign_keys = 1;

-- DROP TABLE IF EXISTS requests;
-- DROP TABLE IF EXISTS users;
-- DROP TABLE IF EXISTS entrys;
-- DROP TABLE IF EXISTS useragents;

CREATE TABLE IF NOT EXISTS entrys (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp       BIGINT          NOT NULL,
  request_id      INTEGER         NOT NULL,
  user_id         INTEGER         NOT NULL,
  -- referrer is intentionally nullable
  referrer_id     INTEGER,        
  FOREIGN KEY (request_id) REFERENCES requests(id),
  FOREIGN KEY (user_id) REFERENCES users(id),
  FOREIGN KEY (referrer_id) REFERENCES referrers(id),
  UNIQUE (timestamp, request_id, user_id)
);
CREATE INDEX IF NOT EXISTS entrys_cols ON entrys(timestamp, request_id, user_id);

CREATE TABLE IF NOT EXISTS requests (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  method          TEXT      NOT NULL,
  url             TEXT   NOT NULL,
  status_code     INTEGER         NOT NULL,
  UNIQUE (method, url, status_code)
);
CREATE INDEX IF NOT EXISTS requests_cols ON requests(method, url, status_code);

CREATE TABLE IF NOT EXISTS users (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,

  -- hash intentionally allows NULL, so you can forget the user hash
  hash            BIGINT UNIQUE,

  -- useragent_id intentionally allows NULL, so you can forget the useragent
  useragent_id    INTEGER,
  FOREIGN KEY(useragent_id) REFERENCES useragents(id)
);
CREATE INDEX IF NOT EXISTS users_cols ON users(hash, useragent_id);

CREATE TABLE IF NOT EXISTS useragents (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  value           TEXT    NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS useragents_value ON useragents(value);

CREATE TABLE IF NOT EXISTS referrers (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  url             TEXT     NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS referrer_url ON referrers(url);

CREATE TABLE IF NOT EXISTS countries (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  code            TEXT      NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS country_code ON countries(code);
