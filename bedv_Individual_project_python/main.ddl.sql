/*
bedv_stg_transactions
bedv_stg_passport_blacklist
bedv_stg_terminals
bedv_stg_cards
bedv_stg_accounts
bedv_stg_clients

bedv_dwh_fact_transactions
bedv_dwh_fact_passport_blacklist
bedv_dwh_dim_terminals_hist
bedv_dwh_dim_cards_hist
bedv_dwh_dim_accounts_hist
bedv_dwh_dim_clients_hist

bedv_stg_del_terminals
bedv_stg_del_cards
bedv_stg_del_accounts
bedv_stg_del_clients

bedv_meta
bedv_rep_fraud
*/


drop table de11an.bedv_dwh_fact_transactions;
drop table de11an.bedv_stg_transactions;

							update de11an.bedv_dwh_dim_cards_hist
								set effective_to = tmp.update_dt - interval '1 second'
							from (
								select 
								stg.card_num, 
								stg.update_dt
								from de11an.bedv_stg_cards stg
								inner join de11an.bedv_dwh_dim_cards_hist tgt
									on stg.card_num = tgt.card_num
									and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
										and tgt.deleted_flg = 'N'
								where 1=0 
									or stg.card_num <> tgt.card_num or (stg.card_num is null and tgt.card_num is not null) or (stg.card_num is not null and tgt.card_num is null) 
									or stg.account_num <> tgt.account_num or (stg.account_num is null and tgt.account_num is not null) or (stg.account_num is not null and tgt.account_num is null)
									) tmp
							where de11an.bedv_dwh_dim_cards_hist.card_num = tmp.card_num
								and de11an.bedv_dwh_dim_cards_hist.deleted_flg = 'N' 
							;

/*
delete from de11an.bedv_dwh_fact_transactions;
delete from de11an.bedv_dwh_fact_passport_blacklist;
delete from de11an.bedv_dwh_dim_terminals_hist;
delete from de11an.bedv_dwh_dim_cards_hist;
delete from de11an.bedv_dwh_dim_accounts_hist;
delete from de11an.bedv_dwh_dim_clients_hist;
delete from de11an.bedv_meta;
*/

/*
update de11an.bedv_meta	set max_update_dt = to_date('2021-02-28', 'YYYY-MM-DD') where table_name = 'bedv_stg_transactions';
update de11an.bedv_meta	set max_update_dt = to_date('1899-01-01', 'YYYY-MM-DD') where table_name = 'bedv_stg_cards';
update de11an.bedv_meta	set max_update_dt = to_date('1899-01-01', 'YYYY-MM-DD') where table_name = 'bedv_stg_accounts';
update de11an.bedv_meta	set max_update_dt = to_date('1899-01-01', 'YYYY-MM-DD') where table_name = 'bedv_stg_clients';
*/

--TRUNCATE table de11an.bedv_meta;
select * from de11an.bedv_meta;

select * from de11an.bedv_dwh_fact_transactions;
select count(1) from de11an.bedv_dwh_fact_transactions;
select count(1) from de11an.bedv_dwh_fact_passport_blacklist;
SELECT * FROM pg_stat_activity WHERE state = 'active';
SELECT pg_cancel_backend(3599346);
SELECT pg_terminate_backend(3599346);

select * from de11an.bedv_dwh_fact_transactions;
select * from bedv_dwh_fact_passport_blacklist;
select * from de11an.bedv_dwh_dim_terminals_hist;
select * from de11an.bedv_dwh_dim_cards_hist;
select * from de11an.bedv_dwh_dim_accounts_hist;
select * from de11an.bedv_dwh_dim_clients_hist;
select * from de11an.bedv_meta;


/*
Признаки мошеннических операций.
1. Совершение операции при просроченном или заблокированном паспорте.
2. Совершение операции при недействующем договоре.
3. Совершение операций в разных городах в течение одного часа.
4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, 
при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.

event_dt Время наступления события. Если событие наступило по результату нескольких действий – указывается время действия, по которому установлен факт мошенничества.
passport Номер паспорта клиента, совершившего мошенническую	операцию.
fio	ФИО клиента, совершившего мошенническую операцию.
phone	Номер телефона клиента, совершившего мошенническую	операцию.
event_type	Описание типа мошенничества (номер).
report_dt	Дата, на которую построен отчет.
*/

--удаляем лишние пробелы
--update de11an.bedv_dwh_dim_cards_hist set card_num = trim(card_num);
delete from de11an.bedv_rep_fraud;

select * from de11an.bedv_rep_fraud;

--SET AUTOCOMMIT TO off;
--SET AUTOCOMMIT TO on;

--1. Совершение операции при просроченном или заблокированном паспорте.
insert into de11an.bedv_rep_fraud( event_dt, passport, fio, phone, event_type, report_dt)
select distinct
	tr.trans_date as event_dt, 
	cli.passport_num as passport, 
	(cli.last_name || ' ' || cli.first_name || ' ' || cli.patronymic) as fio, 
	cli.phone as phone,
	1 as event_type,
	now()::date as report_dt
from de11an.bedv_dwh_fact_transactions tr
inner join de11an.bedv_dwh_dim_cards_hist crd on crd.card_num = tr.card_num
inner join de11an.bedv_dwh_dim_accounts_hist acc on acc.account_num = crd.account_num
inner join de11an.bedv_dwh_dim_clients_hist cli on cli.client_id = acc.client
left join de11an.bedv_dwh_fact_passport_blacklist pbl on pbl.passport_num = cli.passport_num
where (tr.trans_date::date > coalesce(cli.passport_valid_to, now())	or tr.trans_date>pbl.entry_dt)
	and now()::date> coalesce((select max(report_dt) from de11an.bedv_rep_fraud where event_type=1), now()::date-1)
;
--2. Совершение операции при недействующем договоре.
insert into de11an.bedv_rep_fraud( event_dt, passport, fio, phone, event_type, report_dt)
select distinct
	tr.trans_date as event_dt, 
	cli.passport_num as passport, 
	(cli.last_name || ' ' || cli.first_name || ' ' || cli.patronymic) as fio, 
	cli.phone as phone,
	2 as event_type,
	now()::date as report_dt
from de11an.bedv_dwh_fact_transactions tr
inner join de11an.bedv_dwh_dim_cards_hist crd on crd.card_num = tr.card_num
inner join de11an.bedv_dwh_dim_accounts_hist acc on acc.account_num = crd.account_num
inner join de11an.bedv_dwh_dim_clients_hist cli on cli.client_id = acc.client
where tr.trans_date::date > acc.valid_to 
	and now()::date> coalesce((select max(report_dt) from de11an.bedv_rep_fraud where event_type=2), now()::date-1)
; 
--3. Совершение операций в разных городах в течение одного часа (по одной и той же card_num)
--insert into de11an.bedv_rep_fraud( event_dt, passport, fio, phone, event_type, report_dt)
select distinct
	tr.trans_date as event_dt, 
	cli.passport_num as passport, 
	(cli.last_name || ' ' || cli.first_name || ' ' || cli.patronymic) as fio, 
	cli.phone as phone,
	3 as event_type,
	now()::date as report_dt
from de11an.bedv_dwh_fact_transactions tr
inner join de11an.bedv_dwh_dim_cards_hist crd on crd.card_num = tr.card_num
inner join de11an.bedv_dwh_dim_accounts_hist acc on acc.account_num = crd.account_num
inner join de11an.bedv_dwh_dim_clients_hist cli on cli.client_id = acc.client
inner join de11an.bedv_dwh_dim_terminals_hist trm on trm.terminal_id = tr.terminal_id
where tr.trans_date::date > acc.valid_to 
	and now()::date> coalesce((select max(report_dt) from de11an.bedv_rep_fraud where event_type=3), now()::date-1)
; 

--4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей, 
--при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.

--В модели настраиваются пути к source и archive (по умолчанию - в той же папке, что и скрипт .py)
--Модель настроена на SCD2
--Модель корректно обрабатывает ВСЕ файлы, которые есть в папке sourse и складывает их в archive
--Модель правильно отрабатывает при повторном запуске скрипта, можно безопасно запускать несколько раз (есть ограничения повторной заливки дубликатов - по датам формирования отчета)
--Модель правильно обрабатывает данные с лишними начальныими и конечными пробелами в sourse (корректируются в момент загрузки)
--реализована кодогенерация единой схемы SCD2 для разных источников (файл, SQL-таблицы)
--реализована построчная заливка данных с одной базы, в другую через курсор (не используя Пандас), что экономит память и не дает упасть скрипту при больших таблицах.
--реализован механизм проверки ошибок - полного инкремента по дням. Отсутствующий день будет ожидатся, после чего загрузятся остальные, что означает отсутствие ошибки - пропуска дней, в случаях когда их положили в сорс, а в мете уже дата больше и она не подгрузжаются.
--транзакция закрывается только после удачного выполнения SQL-команд и сохранения файлов в архив (для возможности повторного запуска, в случае ошибки) 
--дописать еще...
--stg
create table de11an.bedv_stg_transactions( 
	trans_id varchar,
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar,
	amt decimal,
	oper_result varchar,
	terminal varchar
);
create table de11an.bedv_stg_passport_blacklist( 
	passport_num varchar(15),
	entry_dt date
);
create table de11an.bedv_stg_terminals( 
	terminal_id varchar,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	update_dt timestamp(0)
);
create table de11an.bedv_stg_cards( 
	card_num varchar(20),
	account_num varchar(20),
	update_dt timestamp(0)
);
create table de11an.bedv_stg_accounts( 
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	update_dt timestamp(0)
);
create table de11an.bedv_stg_clients( 
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	update_dt timestamp(0)
);

--dwh
create table de11an.bedv_dwh_fact_transactions( 
	trans_id varchar,--integer,
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar,
	amt decimal,
	oper_result varchar,
	terminal varchar
);
create table de11an.bedv_dwh_fact_passport_blacklist( 
	passport_num varchar(15),
	entry_dt date
);
create table de11an.bedv_dwh_dim_terminals_hist( 
	terminal_id varchar,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg char(1)
);
create table de11an.bedv_dwh_dim_cards_hist( 
	card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg char(1)
);
create table de11an.bedv_dwh_dim_accounts_hist( 
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg char(1)
);
create table de11an.bedv_dwh_dim_clients_hist( 
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg char(1)
);

--stg_del
create table de11an.bedv_stg_del_terminals( 
	terminal_id varchar
);
create table de11an.bedv_stg_del_cards( 
	card_num varchar(20)
);
create table de11an.bedv_stg_del_accounts( 
	account_num varchar(20)
);
create table de11an.bedv_stg_del_clients( 
	client_id varchar(10)
);

--meta+repopt
create table de11an.bedv_meta (
	schema_name varchar,
	table_name varchar,
	max_update_dt timestamp(0)
);
create table de11an.bedv_rep_fraud (
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar,
	phone varchar(16),
	event_type integer,
	report_dt date
);

--заполняем мету разово начальными датами
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_transactions', to_date('2021-02-28', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_passport_blacklist', to_date('2021-02-28', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_terminals', to_date('2021-02-28', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_cards', to_date('1899-01-01', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_accounts', to_date('1899-01-01', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_clients', to_date('1899-01-01', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_rep_fraud', to_date('1899-01-01', 'YYYY-MM-DD'));


