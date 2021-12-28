PRAGMA foreign_keys=1;

DROP TABLE IF EXISTS entrys;
CREATE TABLE entrys (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp       BIG INT         NOT NULL,
  request_id      INTEGER         NOT NULL,
  user_id         INTEGER         NOT NULL,
  FOREIGN KEY (request_id) REFERENCES requests(id),
  FOREIGN KEY (user_id) REFERENCES users(id),
  UNIQUE(timestamp, request_id, user_id)
);

DROP TABLE IF EXISTS requests;
CREATE TABLE requests (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  method          VARCHAR(7)      NOT NULL,
  url             VARCHAR(2048)   NOT NULL,
  status_code     INTEGER         NOT NULL,
  UNIQUE(method, url, status_code)
);

DROP TABLE IF EXISTS users;
CREATE TABLE users (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  hash            VARCHAR(512)   NOT NULL UNIQUE,
  useragent_id    INTEGER        NOT NULL,
  FOREIGN KEY(useragent_id) REFERENCES useragents(id)
);

DROP TABLE IF EXISTS useragents;
CREATE TABLE useragents (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  value      VARCHAR(2048)      NOT NULL UNIQUE
);