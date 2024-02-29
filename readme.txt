install the following libraries
pandas
tabulate
the above libraries can be installed from requirements.txt
you can run 
python custom_terminal.py 
The above command runs the terminal to execute the commands

The below are some sample queries that can be used for testing 
select * from players limit 10
select player_id, name, country_of_birth from players limit 50
select player_id, name, country_of_birth from players where player_id > 10 and player_id < 1000
select player_id, name, country_of_birth from players where first_name == "David"
select player_id, name from players where player_id > 1000 and player_id < 20000 order_by name desc limit 100
select player_id, name, country_of_birth from players where first_name == "David" order_by player_id desc
select first_name, count(first_name) from players group_by first_name having count(first_name) > 1 order_by count(first_name) desc limit 100
select country_of_birth, count(*) from players group_by country_of_birth order_by count(*) desc limit 40
select players.player_id, players.name, valuations.market_value_in_eur from players inner_join players.player_id = valuations.player_id limit 50
select players.player_id, players.name, valuations.market_value_in_eur from players inner_join players.player_id = valuations.player_id where valuations.market_value_in_eur > 1000000 and players.player_id > 1000 order_by valuations.market_value_in_eur desc limit 40
select players.country_of_birth, count(players.player_id), sum(valuations.market_value_in_eur) from players inner_join players.player_id = valuations.player_id where valuations.market_value_in_eur > 1000000 and players.player_id > 1000 group_by players.country_of_birth having sum(valuations.market_value_in_eur) > 2000000 order_by sum(valuations.market_value_in_eur) desc limit 40

DDL commands
create table test_data id int name str salary float primary_key id
insert into test_data columns id, name, salary values 1, "John Doe", 100000.0
update test_data set salary values 200000 where id == 1
delete from test_data where id == 1
drop table test_data