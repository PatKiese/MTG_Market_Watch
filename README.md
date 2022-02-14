# MTG_Market_Watch

## About this Project
I think everyone that starts at the bottom field of Data Engineering/Data Science has a hard time when it comes to starting an own first private project. 
Questions such as "what kind of data do I use?", "how to handle data-transformation, -storage and -presentation?" or even "what tools are available" can be overwhelming.
So I did what every desperate beginner does nowadays - I checked reddit, stackoverflow and the likes. Reading several blogs and watching dozens of YouTube tutorial I
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
TBD
### Data transformation
TBD
### Data presentation
TBD

## TODO's and known issues
- Adding more descriptions to the code and to this ReadMe :)
- Prettify the tables in the flask app e.g. hoverable images of the cards and plots.
- Introducing OOP principles for better structure and encapsulation. Right now the code is more or less a collection of methods and scripts.
- Slim down the css part. Currently using a template from [html5up.net](https://html5up.net)
