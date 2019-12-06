
drop table zapier.jkirkpatrick.copy_data;
drop table zapier.jkirkpatrick.new_data;

-- copy data into working directory and create unique identifier
select *,
       account_id::text || '_' || user_id as account_user -- both are bigints so had to convert one to text
into zapier.jkirkpatrick.copy_data
from zapier.source_data.tasks_used_da;


create table zapier.jkirkpatrick.new_data as

with

-- smaller set of records to test with
subset (date, user_id, account_id, sum_tasks_used, account_user) as (
    select date,
           user_id,
           account_id,
           sum_tasks_used,
           copy_data.account_user
    from jkirkpatrick.copy_data
    limit 100000),

-- list of all dates in table jan 1 2017 thru june 1 2017
  date_list (date) as (
      select distinct
                    date
      from jkirkpatrick.copy_data
      order by date),

-- list of all unique users
  user_list (account_user, account_id, user_id) as (
      select distinct
                      account_user,
                      account_id,
                      user_id
          from jkirkpatrick.copy_data),

-- join for all combinations of date x user
   dates_users (account_user, date) as (
          select
                 ul.account_user,
                 dl.date
          from date_list as dl
                cross join user_list as ul
          order by account_user, date),

-- table of users and the date of their first task
 first_obs (account_user, account_id, user_id, first_obs_date) as (
     select
            cd.account_user,
            cd.account_id,
            cd.user_id,
            cd.first_obs_date
          from (select
                       account_user,
                       account_id,
                       user_id,
                       date as first_obs_date,
                       row_number() over (partition by account_user order by date) as seqnum
                from zapier.jkirkpatrick.copy_data) cd
                where seqnum = 1),

-- join first task date info with the full dateXuser table, remove obs prior to first task
  first_obs_join (account_user, date, first_obs_date) as (
          select
                 du.account_user,
                 du.date,
                 fo.first_obs_date,
                 datediff(days, first_obs_date, date) as date_diff_calc
          from dates_users as du
              full join first_obs as fo on du.account_user = fo.account_user
          where date_diff_calc>=0
          order by account_user, date),

-- join original data with day-user-tasks info with table of all users and of all dates on/after user's first observation
-- for each user, any date in first_obs_join that isn't in copy_data, fill in sum_tasks_used as 0
full_set (account_user, account_id, user_id, date, first_obs_date, sum_tasks_used) as (
          select
                 cd.account_user,
                 cd.account_id,
                 cd.user_id,
                 foj.date,
                 foj.first_obs_date,
                 coalesce(cd.sum_tasks_used, 0) as sum_tasks_used
          from first_obs_join as foj
                inner join jkirkpatrick.copy_data as cd
                on foj.account_user = cd.account_user and foj.date = cd.date),

-- calculate rolling sums for active/churn assignment
  calcd (account_user, account_id, user_id, date, first_obs_date, sum_tasks_used, roll_sum_28, roll_sum_56) as (
      select
             account_user,
             account_id,
             user_id,
             date,
             first_obs_date,
             sum_tasks_used,
             sum(full_set.sum_tasks_used) over (order by account_user, date rows 27 preceding) as roll_sum_28,
             sum(full_set.sum_tasks_used) over (order by account_user, date rows 55 preceding) as roll_sum_56
      from full_set
      order by account_id, user_id, date)

-- assign active and churn
select
       account_user,
       account_id,
       user_id,
       date,
       first_obs_date,
       sum_tasks_used,
       roll_sum_28,
       roll_sum_56,
       case
           when roll_sum_28>0 then 'active'
           when roll_sum_28=0 and roll_sum_56>0 then 'churn'
           else null
      end as status
from calcd
where roll_sum_56>0
order by account_id, user_id, date;



select * from jkirkpatrick.new_data limit 100;







