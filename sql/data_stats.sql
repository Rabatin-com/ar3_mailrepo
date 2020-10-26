/* SQL SERVER */
SELECT email_account as 'Email Account', 
max(msg_ts) as 'Latest Date', 
count(id) as 'Count'  
FROM messagedata group by email_account order by email_account
