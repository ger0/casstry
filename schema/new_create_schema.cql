CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE Lists (
  name varchar,
  max_size int,
  students list<int>,
  PRIMARY KEY (name)
);

CREATE TABLE Proposals (
    student_id int,
    list_name varchar,
    placements list<int>
    PRIMARY KEY(student_id, list_name)
);