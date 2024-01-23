CREATE KEYSPACE IF NOT EXISTS my_cass
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE Lists (
  name varchar,
  max_size int,
  students map<int, int>,
  timestamps map<int, timestamp>,
  PRIMARY KEY (name)
);


CREATE TABLE Proposals (
    student_id int,
    list_name varchar,
    placements list<int>,
    sending_time timestamp,
    PRIMARY KEY(student_id, list_name)
);