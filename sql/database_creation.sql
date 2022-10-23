CREATE DATABASE globant;
USE globant;
-- drop database globant;

CREATE TABLE departments (
    id int PRIMARY KEY NOT NULL, 
    department varchar(45) NOT NULL
);

CREATE TABLE jobs (
    id int PRIMARY KEY NOT NULL, 
    job varchar(45) NOT NULL
);

CREATE TABLE hired_employees (
    id INT PRIMARY KEY NOT NULL, 
    name varchar(45) NOT NULL,
    datetime varchar(45) NOT NULL,
    department_id INT NOT NULL,
    job_id INT NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id)
);