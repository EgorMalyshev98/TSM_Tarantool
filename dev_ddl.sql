CREATE SCHEMA dev_application AUTHORIZATION admin_user;
-- Норматив
CREATE TABLE dev_application.dev_app__norm (
	"id" serial4 not null, 
	"operation_type" varchar, --Тип операции
	"operation_name" varchar, --Наименование операции
	"unit" varchar, --Единицы измерения
	"technique_type" varchar, --Тип техники
	"technique_name" varchar, --Наименование техники
	"num_of_tech" int, --Количество техники в звене
	"workload_1000_units" float8, --Трудоемкость на 1000 единиц объема
	CONSTRAINT pk_dev_app__norm PRIMARY KEY (id)
);

-- Наличие техники
CREATE TABLE dev_application.dev_app__available_tech (
	"id" serial4 not null,
	"date" date, --Дата
	"technique_type" varchar, --Тип техники
	"technique_name" varchar, --Наименование техники
	"quantity" int, --Наличие, единиц
	"shift_work" int, --Сменность работы
	"workload_month" float8, --Трудоемкость (месячная)
	CONSTRAINT pk_dev_app__available_tech PRIMARY KEY (id)
);

-- Контрактная ведомость
CREATE TABLE dev_application.dev_app__contract (
	"id" serial4 not null,
	"num_con" varchar, --№КВ
	"work_name" varchar, --Наименование работы
	"unit" varchar, --Единица измерения
	"vol" float8, --Объем
	"price" float8, --Расценка, руб
	"cost" float8, --Стоимость, руб
	CONSTRAINT pk_dev_app__contract PRIMARY KEY (id)
);

-- П/РД
CREATE TABLE dev_application.dev_app__prd (
	"id" serial4 not null,
	"num_prd" varchar, --№ П/РД
	"operation_type" varchar, --Тип операции
	"num_con" varchar, --№КВ
	"work_name" varchar, --Наименование работы
	"unit" varchar, --Единица измерения
	"picket_start" float8, --Пикетаж начала
	"picket_finish" float8, --Пикетаж конец
	"length" float8, --Протяженность
	"vol_prd" float8, --Объем П/РД
	CONSTRAINT pk_dev_app__prd PRIMARY KEY (id)
);

-- Факт
CREATE TABLE dev_application.dev_app__fact (
	"id" serial4 not null,
	"date" date, --Дата
	"operation_type" varchar, --Тип операции
	"ispol" varchar, --Исполнитель
	"num_con" varchar, --№КВ
	"work_name" varchar, --Наименование работы
	"unit" varchar, --Единица измерения
	"picket_start" float8, --Пикетаж начала
	"picket_finish" float8, --Пикетаж конец
	"length" float8, --Протяженность
	"vol_fact" float8, --Объем выполненный Факт
	CONSTRAINT pk_dev_app__fact PRIMARY KEY (id)
);

-- Технология производства работ
CREATE TABLE dev_application.dev_app__technology (
	"id" serial4 not null,
	"hierarchy" int, --Иерархия технологии
	"operation_type" varchar, --Тип операции
	"work_name" varchar, --Наименование работы
	"unit" varchar, --Единица измерения
	CONSTRAINT pk_dev_app__technology PRIMARY KEY (id)
);
