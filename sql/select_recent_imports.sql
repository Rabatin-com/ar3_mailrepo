/* SQL SERVER */
SELECT TOP (100)
      [email_account]
	  ,[dnload_ts]
      ,[msg_id]
      ,[msg_ts]
      ,[msg_subj]
      ,[msg_from]      
      ,[raw_data]
      ,[gmail_data]
  FROM [ar3_mailrepo].[dbo].[messagedata] where email_account  = 'rabatin@protonmail.ch' order by msg_ts desc