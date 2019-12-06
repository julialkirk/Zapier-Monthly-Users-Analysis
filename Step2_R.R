
### Zapier User Churn Analysis ###
### Julia Kirkpatrick ###
#### December 2019 ###

# ipak function: install and load multiple R packages.
# check to see if packages are installed. Install them if they are not, then load them into the R session.
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}
packages <- c("RPostgreSQL", "tidyverse", "padr", "data.table", "zoo", "psych", 
              "survival", "survminer", "ggfortify", "ranger")
ipak(packages)



# connect to database
drv <- dbDriver("PostgreSQL")
conn <- dbConnect(drv, host="public-redshift.zapier.com", 
                  port="5439",
                  dbname="zapier", 
                  user="jkirkpatrick", 
                  password="jkirkpatrick1234A")
dbGetQuery(conn, "select count(*) from source_data.tasks_used_da;")
dat <- dbGetQuery(conn, "select * from source_data.tasks_used_da")
dbDisconnect(conn)

###################################################
### *** this was supposed to be done in SQL *** ###
###################################################
# pad where dates are missing for account/users
end_of_data_date <- max(dat$date)
dat2 <- dat %>%
  group_by(account_id, user_id) %>%
  arrange(account_id, user_id, date) %>%
  pad(end_val = end_of_data_date, break_above = 40) %>%
  fill_by_value(value=0) 

# create logic to assign user status - active vs churn
dat3 <- dat2 %>%
  group_by(account_id, user_id) %>%
  arrange(account_id, user_id, date) %>%
  mutate(account_user = paste(account_id, user_id, sep="_"),
         obs_num = row_number(),
         any_task = ifelse(sum_tasks_used>0, 1, 0),
         cumsum_tasks = cumsum(sum_tasks_used),
         roll_sum_28 = rollapply(data=sum_tasks_used, width=28, FUN=sum, align = "right", partial=TRUE),
         roll_sum_56 = rollapply(data=sum_tasks_used, width=56, FUN=sum, align = "right", partial=TRUE),
         status = ifelse(roll_sum_28>0, "active", 
                         ifelse(roll_sum_28==0, "churn", NA))) %>%
  mutate(churned = ifelse(status=="churn", 1, 0)) %>%
  filter(roll_sum_56>0) # remove all records where user is beyond churn period (not using in this analysis)

###################################################
###################################################
###################################################


#######################################################################################
### Below: the portion I would have completed in R, assuming "dat3" is what I was able 
### to accomplish in SQL. Much if this is still less efficient than I'd like!
#######################################################################################

# find churn users' date of first churn
first_churn <- dat3 %>%
  filter(status=="churn") %>%
  arrange(date) %>%
  group_by(account_id, user_id) %>%
  summarise(first_churn = min(date)) %>%
  unique() # if users churn, become active, churn again, this selects only the date of first churn (top value)

# merge with full dataset - will contain all users
sa <- merge(dat3, first_churn, by=c("account_id", "user_id"), all=TRUE) 

# calculate avg tasks per day and pct days with task *while status=active* (one obs/user)
# first calc for those who never churned (a) - don't want to calculate for full dataset at once, 
# because grouping by account/user would include the churned user's churn days in avg calc
summary_never_churned <- sa %>%
  filter(is.na(first_churn)) %>% 
  group_by(account_id, user_id) %>%
  summarise(avg_tasks_day = mean(sum_tasks_used),
            pct_days_with_task = (sum(any_task)/n())*100,
            start_date = min(date),
            end_date = max(date),
            tenure = max(obs_num),
            churned = 0)

# calculate avg tasks per day and pct days with task for those who have churned during their active period (b)
summary_churned <- sa %>%
  filter(!is.na(first_churn), status=="active", date<first_churn) %>%
  group_by(account_id, user_id) %>%
  summarise(avg_tasks_day = mean(sum_tasks_used),
            pct_days_with_task = (sum(any_task)/n())*100,
            start_date = min(date),
            end_date = max(date)+1,
            tenure = max(obs_num)+1,
            churned = 1)

# rbind for full avg tasks per user per a.active with b.first active period
unique_user_level <- rbind(summary_never_churned, summary_churned)

# identify day of week of first task for all users (potential hypothesis)
unique_user_level$first_task_weekday <- weekdays(unique_user_level$start_date)

# rename for easier coding
dat <- unique_user_level

# remove unnecessary data
rm(conn, dat2, dat3, drv, first_churn, sa, summary_churned,
   summary_never_churned, end_of_data_date, unique_user_level)
gc()

### data management/reshaping complete ###
##########################################




#########################
### Survival Analysis ###
#########################

# ALL users are active days 1 through 28
# begin observation on day 28. 
# 100% of users are active on day 28
# remove users who have less than 28 days tenure / keep only those whose tenure is at or above
dat2 <- dat[dat$tenure>=28,]

# recode tenure so day 28 = day 1 = time 0
# tenure - 27 = day 28 is now day 1. day 2 we will see who churns - those who executed task on day 1 and never again
dat2$tenure <- dat2$tenure-27

# Survival object
km <- with(dat2, Surv(time=tenure, event=churned, type="right"))
head(km, 20)
# kaplan meier estimate of survival object
km_fit <- survfit(km ~ 1, data=dat2)

# kaplan meier plot
plot(km_fit, xlab="days", ylab="overall retention probability")
ggsurvplot(fit=km_fit, risk.table="abs_pct", xlab="Days Active", ylab="Retention")


# graphical presentation of estimated quantiles (based on the survival curve using k-m)
mc <- data.frame(q = c(.25, .5, .75), # central tendency measures
                 km = quantile(km_fit)) # .75 NA because we don't lose 75% by end of study period

km_fit <- survfit(km ~ 1, data=dat)
ggsurvplot(km_fit, xlab = "Time (days)", censor = F)$plot +
  geom_segment(data = mc, aes(x = km.quantile, y = 1-q, xend = km.quantile, yend = 0), lty = 2) +
  geom_segment(data = mc, aes(x = 0, y = 1-q, xend = km.quantile, yend = 1-q), lty = 2)

# half of the users estimated to have tenure shorter than 51 days (considered churn on day 51+27)
# a quarter churn within 8 days (considered churn on day 8+27)

# what about dropoff at second day?
tt <- table(cut(x = dat2$tenure, breaks = seq(from = 0, to = 26, by = 1)))
tt
270100 - cumsum(tt) # approximate attrition rate, only valid for first few days (because of censoring) but gives idea of magnitude


# avg_tasks_day x churn
describe(dat2$avg_tasks_day) # heavily right skewed
dat2$log_avg_tasks_day <- log(dat2$avg_tasks_day)
ggplot(dat2, aes(x=log_avg_tasks_day, y=..density..)) + geom_density() + theme_classic()
ggplot(dat2, aes(x=log_avg_tasks_day, y=..density.., fill=factor(churned))) + 
  geom_density(alpha=.75) + theme_classic() + scale_fill_discrete(name = "Churned", labels=c("No", "Yes")) 
describeBy(dat2$avg_tasks_day, dat2$churned)

# try to remove outliers
quantile(dat2$avg_tasks_day, c(seq(from=0, to=1, by=.05))) 
# remove top 5% of avg_tasks_day, avg_tasks_day>34

dat3 <- dat2[dat2$avg_tasks_day<=34,]
describe(dat3$avg_tasks_day) # still heavily right skewed
ggplot(dat3, aes(x=avg_tasks_day, y=..density..)) + geom_density() + theme_classic()
dat3$log_avg_tasks_day <- log(dat3$avg_tasks_day)
ggplot(dat3, aes(x=log_avg_tasks_day, y=..density.., fill=factor(churned))) + 
  geom_density(alpha=.75) + theme_classic() + scale_fill_discrete(name = "Churned", labels=c("No", "Yes")) 
describeBy(dat3$avg_tasks_day, dat2$churned)



# pct_days_with_task x churn
describe(dat2$pct_days_with_task)
describeBy(dat2$pct_days_with_task, dat2$churned)
ggplot(dat2, aes(x=pct_days_with_task, y=..density.., fill=factor(churned))) + 
  geom_density(alpha=.75) + theme_classic() + scale_fill_discrete(name = "Churned", labels=c("No", "Yes")) 


# day of week
prop.table(table(dat2$first_task_weekday, dat2$churned),1)
table(dat2$first_task_weekday)
ggsurvplot(survfit(km ~ first_task_weekday, data=dat2),
           data=dat2, censor=FALSE, conf.int=TRUE,
           title="Retention by First Task Day of Week", legend="right",
           font.title=11, font.x=9, font.y=9, font.tickslab=8, font.legend=7)




### Building the Model ###
fit_cox <- coxph(Surv(time=tenure, event=churned, type="right") ~
                   log_avg_tasks_day + pct_days_with_task + first_task_weekday,
                 data=dat3)
# use log_avg_tasks_day because cox is semi-parametric - 
# - outcome does not need to follow distribution but predictors do
options("scipen"=999, "digits"=4)
summary(fit_cox) # avg_tasks_daily and pct_days_with_task should not have opposite effect on churn likelihood

# predictors are TIME VARYING - violates an assumption of the cox model. 
# effect estimates are wonky
# can use km survival object to fit a different survival model



### Use Ranger ### - random forest implementation of survival analysis
# time varying still an issue, but less so b/c not parameterized 
set.seed(42)
samp <- dat2[sample(nrow(dat2), 25000), ]

ranger_fit <- ranger(Surv(tenure, churned) ~ 
                       avg_tasks_day + pct_days_with_task + 
                       first_task_weekday,                     
                     data = samp,                     
                     mtry = 1,                     
                     importance = "permutation",                     
                     splitrule = "extratrees",                     
                     verbose = TRUE)

var_imp <- data.frame(sort(ranger_fit$variable.importance, decreasing = TRUE))
var_imp



# calculate something like concordence/ROC curve (c-index)
1 - ranger_fit$prediction.error










