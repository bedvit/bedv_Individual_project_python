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


delete from de11an.bedv_dwh_dim_cards_hist;
delete from de11an.bedv_dwh_fact_passport_blacklist;
delete from de11an.bedv_meta;
--TRUNCATE table de11an.bedv_meta;
select * from de11an.bedv_meta;
select * from de11an.bedv_stg_transactions;
select * from de11an.bedv_stg_transactions;
select count(1) from de11an.bedv_dwh_fact_transactions;
select count(1) from de11an.bedv_dwh_fact_passport_blacklist;
SELECT * FROM pg_stat_activity WHERE state = 'active';
SELECT pg_cancel_backend(3599346);
SELECT pg_terminate_backend(3599346);

select * from de11an.bedv_dwh_dim_terminals_hist;

select * from bedv_dwh_fact_passport_blacklist;
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

select * from de11an.bedv_rep_fraud;
--insert into de11an.bedv_rep_fraud( event_dt, passport, fio, phone, event_type, report_dt)
select 
	tr.trans_date, 
	cli.passport_num, 
	(cli.last_name || ' ' || cli.first_name), 
	cli.phone,
	2,
	now()
from de11an.bedv_dwh_fact_transactions tr
inner join de11an.bedv_dwh_dim_cards_hist crd on crd.card_num = tr.card_num
inner join de11an.bedv_dwh_dim_accounts_hist acc on acc.account_num = crd.account_num
inner join de11an.bedv_dwh_dim_clients_hist cli on cli.client_id = acc.client
where tr.card_num ='4513 5880 2369 1799';

select * 
from de11an.bedv_dwh_dim_cards_hist crd
--where crd.card_num ='4513 5880 2369 1799';

--stg
create table de11an.bedv_stg_transactions( 
	trans_id varchar,
	trans_date date,
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
	trans_date date,
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


