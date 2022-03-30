# MTG_Market_Watch

## About this Project
I think everyone that starts at the bottom field of Data Engineering/Data Science has a hard time when it comes to starting an own first private project. 
Questions such as "what kind of data do I use?", "how to handle data-transformation, -storage and -presentation?" or even "what tools are available" can be overwhelming.
So I did what every desperate beginner does nowadays - I checked reddit, stackoverflow and the likes. Reading several blogs and watching dozens of YouTube tutorials I
finally gathered the courage to start. It should be something simple, yet complete and cover a thematic that revolves around my interests - stock market analysis (such as 
[#CashTag](https://github.com/shafiab/HashtagCashtag)) seemed to be a popular topic and an ideal starting point. But the stock market didn't quite catch my interesst so I
decided to give it a little twist by combining one of my hobbies with this topic: the trading card game "Magic the Gathering". Here we have a so called secondary market, where 
cards are sold by the simple rules of demand vs. supply (and unlike the stock market it is not regulated :wink:).

## Data pipeline for a pricing analysis on Magic the Gathering cards
MTG_Market_Watch collects secondary market prices of Magic the Gathering cards from the [scryfall API](https://https://scryfall.com/docs/api) and analyses data to detect rapid 
pricing increases ("spikes") within a give time (usualy twice a week).

![This is an image](/assets/Overview.png)

## What it does
### Data ingestion
Scryfall provides a REST-like API for ingesting our card data programatically. The API exposes information available on the regular site in easy-to-consume formats (e.g. JSON).
The site provides its entire database compressed for download in [bulk data](https://scryfall.com/docs/api/bulk-data) files, which will be updated every 24h.
Our "DbLoader" class fetches this bulk data via the "fetch_data" method via a simple get-request and returns a pandas DataFrame. This DataFrame will then be loaded to our local 
MySQL server with the "load_data_to_db" method. Credentials will be managed by the "ConfigManager" class. Credentials and run ids can be provided directly or with JSON files.
### Data transformation
Heart of the data analysis are the DataTransformer and PriceChecker classes. After the bulk data has been prepared, it can be provided to the DataTransformer wich 
will create a more clearly arranged table ("create_pricing_table" method) and appends it into the "pricing_table" SQL table. This table will then act as a basis 
for our calculations. The calculations will be done in the PriceChecker class. It checks for each relevant MtG-format ("standard", "commander", "pioneer", "modern", "legacy") the top ten greatest increases in price which also have a minimum price increase of at least 1USD. The resulting DataFrame will then be saved as a csv-file.

### Data presentation
The resulting data will be provided in a simple flask "dashboard". Starting from the index, on can select the different MtG-formats.
![This is an image](/assets/Index.PNG)

## TODO's and known issues
- Adding more descriptions to the code and to this ReadMe :)
- ~~Prettify the tables in the flask app e.g. hoverable images of the cards and plots.~~
- ~~Introducing OOP principles for better structure and encapsulation. Right now the code is more or less a collection of methods and scripts.~~
- Slim down the css part. Currently using a template from [html5up.net](https://html5up.net)
- Introduction of workflow orchestration (probably Airflow)
- ~~Introduction of small unit-tests~~
- Adding an API to get results as a JSON file
