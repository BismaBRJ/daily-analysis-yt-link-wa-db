# Daily Analysis of YouTube Links from Local WhatsApp Database

**Note:** This project is still ongoing. Stay tuned!

Some of my relatives like to share any interesting video that shows up on their feed. For example, they sometimes come accross a recipe they'd like to try one day! It makes me wonder if I can, say,

- compile a list of foods they like or want to try cooking,

- then find out whether they tend to prefer preparing pastries or heavy meals, 

- whether that preference changes over time, and

- even get a good idea of their feed overall.

That sounds a lot like a data project, and, given my interest in the world of data, thus this project is born!

They tend to watch videos on YouTube and send me messages via WhatsApp. Since I have WhatsApp Desktop installed on my Mac, I tinkered around to find out where and how messages are stored. Turns out, WhatsApp Desktop uses an SQLite database locally on my laptop, which shouldn't be too hard to query. Great!

Unlike the usual Kaggle projects where data is analyzed once and for all, I realize that my relatives send YouTube videos pretty much daily. I ain't doing all that manually every single day, so I sought out to design an ETL pipeline:

1. Extract messages (with additional data, like timestamps) from that SQLite database;

2. Transform the data into insights with PySpark, even MLlib for topic modeling; then

3. Load that data into a different storage, say a Parquet file, from which I can then display a dashboard made with Streamlit.

Oh, and all of that is to be scheduled daily with Airflow.

That way, whenever I want to get up to speed, I don't even need to worry about whether or not the data is outdated; I can just run the Streamlit dashboard and be rest assured that I'm up to date! :D
