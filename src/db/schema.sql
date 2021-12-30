PRAGMA journal_mode = WAL;
-- PRAGMA synchronous = OFF;

-- PRAGMA journal_mode = OFF;
-- PRAGMA foreign_keys = 1;

-- DROP TABLE IF EXISTS requests;
-- DROP TABLE IF EXISTS users;
-- DROP TABLE IF EXISTS entrys;
-- DROP TABLE IF EXISTS useragents;

CREATE TABLE IF NOT EXISTS entrys (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp       BIG INT         NOT NULL,
  request_id      INTEGER         NOT NULL,
  user_id         INTEGER         NOT NULL,
  FOREIGN KEY (request_id) REFERENCES requests(id),
  FOREIGN KEY (user_id) REFERENCES users(id),
  UNIQUE (timestamp, request_id, user_id)
);
CREATE INDEX IF NOT EXISTS entrys_cols ON entrys(timestamp, request_id, user_id);


CREATE TABLE IF NOT EXISTS requests (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  method          VARCHAR(7)      NOT NULL,
  url             VARCHAR(2048)   NOT NULL,
  status_code     INTEGER         NOT NULL,
  UNIQUE (method, url, status_code)
);
CREATE INDEX IF NOT EXISTS requests_cols ON requests(method, url, status_code);

CREATE TABLE IF NOT EXISTS users (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  hash            VARCHAR(32)    NOT NULL UNIQUE,
  useragent_id    INTEGER        NOT NULL,
  FOREIGN KEY(useragent_id) REFERENCES useragents(id)
);
CREATE INDEX IF NOT EXISTS users_cols ON users(hash, useragent_id);

CREATE TABLE IF NOT EXISTS useragents (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  value      VARCHAR(2048)      NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS useragents_value ON useragents(value);