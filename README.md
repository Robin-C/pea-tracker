<h1>PEA Tracker</h1>
<h2>What does this project do?</h2>

This app allows you to track your portfolio performance as well as benchmark it versus a custom index.

PEA, french for Plan Epargne Action, is a special broker account in France with tax advantages.

<h2>What technologies were used to make this project?</h2>

<h3>Extract and load</h3> 

Custom python scripts using yahoo finance API / scrapy (web crawling) loading data to Big Query.

<h3>Transform</h3>
DBT and SQL to transform the data directly inside of Big Query (the T of ELT)

<h3>Dataviz</h3>
Metabase and SQL for the data vizualition part.

Each component was isolated inside of a docker container and booted up with Docker-compose. Everything is hosted on a cheap VPS.

<h3>General diagram</h3>

![Components diagram]([Imgur](https://imgur.com/LEz81NJ.jpg)

<h3>Airflow DAG</h3>

![Airflow dag]([Imgur](https://imgur.com/offfk5l.png) 

<h3>Metabase dashboard</h3>

![Metabase dashboard]([Imgur](https://imgur.com/6LX4H9c.png)
