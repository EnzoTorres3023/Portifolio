create database informatica;
use informatica;
create table cliente(
	id int,
    nome varchar(500)
);
show tables;

alter table cliente add data_nascimento date;

drop table cliente;