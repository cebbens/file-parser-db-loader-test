DROP DATABASE IF EXISTS parser;

DROP USER IF EXISTS 'parser';

CREATE DATABASE parser;

USE parser;

CREATE USER 'parser' IDENTIFIED BY 'parser';

GRANT ALL PRIVILEGES ON parser.* TO 'parser' WITH GRANT OPTION;

FLUSH PRIVILEGES;

-- Enables LOAD DATA LOCAL INFILE on server
SET GLOBAL local_infile = 'ON';

CREATE TABLE IF NOT EXISTS log_entries
(
  id         INT AUTO_INCREMENT PRIMARY KEY,
  date       DATETIME(3)  NOT NULL,
  ip         VARCHAR(15)  NOT NULL,
  request    VARCHAR(100) NOT NULL,
  status     SMALLINT(6)  NOT NULL,
  user_agent VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS blocked_ips
(
  id            INT AUTO_INCREMENT PRIMARY KEY,
  ip            VARCHAR(15)  NOT NULL,
  request_count INT          NOT NULL,
  reason        VARCHAR(100) NOT NULL
);
