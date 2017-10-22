This file will load the selected file into BigQuery. It's not very fast. Sorry about that, but that's a side effect of meaning anyone can use it.

Run upload_to_bq.bat, follow the instructions.

The console window doesn't close at the very end incase it breaks and you need to send me screenshots for debugging. Broadly speaking if you get error messages in the program window that opens up it's my problem, if you get errors in the windows console then you've probably given BQ some bad data and you'll need to fix it (although you can ping me for help of course).

Some quick notes: 

Datasets are like a project in BigQuery. It's hard to work across multiple datasets. I would typically create a project per client.

Different files have different quirks, for example SF crawls need special date processing and STAT exports are all separated by tabs rather than commas. 

Pick whichever file type matches up with yours.

Hopefully the file types are self explanatory except for stat_processed_top_20. This is intended for the STAT top 20 report. It removes the last two columns and changes the column names to rank & ranking_type. 

Why use this? Because you've only used STAT for to scrape Google for a single day. Then we only need one date, so it kills the second date rankings & result types (which for a single days scrape will be empty) to keep things clean.

Debugging:

This program is very forgiving. It will fix a lot of the common errors that can cause BQ to break. After loading data you should sanity check that the schema generated makes sense. For example, if the rank column is a STRING not a INTEGER you won't be able to sum it because it's not all numbers. That means there's a problem with your data, typically because you have uneven columns. For example:

Keyword		 | Rank | Country  | avg_rank  |  device
cars 		 |  2   |	 us    |    4.3    |  mobile
fast cars 	 |  2   |    4.4   |   mobile  | 

In the above example country is missing so all the data in the second row is in the wrong column. This means when you check your schema you will see:

keyword: STRING
rank: INTEGER
country: STRING
average_rank: STRING
device: STRING
