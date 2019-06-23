<h1>Data Lake project for a startup called Sparkify</h1>

<h2>1. purpose of this project</h2>

<p>The purpose of this project is building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.This will allow their analytics team to continue finding insights in what songs their users are listening to.</p>

<h2>2. datacase schema design and ETL pipeline</h2>
This database is star schema that simplifies business reporting and implements fast aggregation for this analysis.

<h4>Fact Table</h4>
<ol>
      <li><strong>songplays</strong> - records in log data associated with song plays i.e. records with page NextSong
            <ul>
                  <li>songplay_id </li>
                  <li>start_time </li>
                  <li>user_id </li>
                  <li>level</li>
                  <li>user_id </li>
                  <li>song_id</li>
                  <li>artist_id</li>
                  <li>session_id</li>
                  <li>location</li>
                  <li>user_agent</li>
            </ul>
      </li>
</ol>

<h4>Dimension Tables</h4>
<ol>
      <li><strong>users</strong> - users in the app
            <ul>
                  <li>user_id </li>
                  <li>first_name</li>
                  <li>last_name</li>
                  <li>gender</li>
                  <li>level</li>
            </ul>
      </li>
      <li><strong>songs</strong> - songs in music database
            <ul>
                  <li>song_id</li>
                  <li>title</li>
                  <li>artist_id</li>
                  <li>year</li>
                  <li>duration</li>
            </ul>
      </li>
      <li><strong>artists</strong> - artists in music database
            <ul>
                  <li>artist_id </li>
                  <li>name</li>
                  <li>location</li>
                  <li>latitude</li>
                  <li>longitude</li>
            </ul>
      </li>
      <li><strong>time</strong> - timestamps of records in songplays broken down into specific units
            <ul>
                  <li>start_time</li>
                  <li>hour</li>
                  <li>day</li>
                  <li>week</li>
                  <li>month</li>
                  <li>year</li>
                  <li>weekday</li>
            </ul>
      </li>
</ol>

<h2>2. Datasets in S3</h2>
<p>Song data: s3://udacity-dend/song_data</p>
<p>Log data: s3://udacity-dend/Log_data</p>

<h2>3. Explanation of the files</h2>
<p><strong>etl.py</strong> - reads song data and log data (json format) from S3 data and create tables that processes using Spark, and then writes them back to S3</p>