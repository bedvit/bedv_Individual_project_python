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
drop table de11an.bedv_stg_terminals;
delete from de11an.bedv_meta;
select * from de11an.bedv_stg_terminals;
select * from de11an.bedv_stg_del_terminals;
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
	create_dt timestamp(0),
	update_dt timestamp(0)
);
create table de11an.bedv_stg_accounts( 
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
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
	create_dt timestamp(0),
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
	deleted_flg integer
);
create table de11an.bedv_dwh_dim_cards_hist( 
	card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg integer
);
create table de11an.bedv_dwh_dim_accounts_hist( 
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg integer
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
	deleted_flg integer
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

--заполняем мету разово начальными датами-
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_transactions', to_date('2021-02-28', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_passport_blacklist', to_date('2021-02-28', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_terminals', to_date('2021-02-28', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_cards', to_date('1899-01-01', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_accounts', to_date('1899-01-01', 'YYYY-MM-DD'));
insert into de11an.bedv_meta (schema_name, table_name, max_update_dt) values ('de11an', 'bedv_stg_clients', to_date('1899-01-01', 'YYYY-MM-DD'));

