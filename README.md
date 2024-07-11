# fashion-resale-data-pipeline

A sample data pipeline to process incoming data from different fashion brands with a variety of different formats, and consolidate into meaningful, normalized and clean datasets. 

Bonus: creates a report with # of items per brand, # of valid items based on required fields, and # of items 'eligible for resale' based on a 'desired profit margin', where items' value decreases based on their value for the 'condition' field


## Directories description

### data/raw

Two mock datasets with secondhand clothing item data from different brands.

I would consider adding a 'data/cleaned' directory if I wanted to save the cleaned data to access later in the code

### src

#### data/extract

Converting .json and .csv files into pyspark dataframes

#### data/process

"Normalizing" data by transforming each brand's dataset to have the similar columns to be used in downstream methods to calculate metrics. Ex: converting column 'title' to 'item' and 'full_price' to 'cost'

Using common methods in the BaseProcessor and creating a tailored Processor class for each brand based on the known format of their datsets (assumming we know their schema beforehand)

#### reporting

_create_report_: calculates and prints metrics based on the data we receive from each brand

_pm_analyzer_: based on the 'quality' of the item (however that is determined on the brand side), a 'desired profit margin' that is changeable, and the 'average cost of processing' each item, calculate a predicted profit margin. 

_Example_: assign different percentage decrease in value based on certain values for 'condition' we know we may receive, decrease the value or 'cost' of the item, subtract processing cost and determine how much we will make from the item should we decide to handle it

#### utils

Just needed to create a udf for pm_analyzer here, would add future udfs there as well

## Installation

Download to your local and run the main.py file from the fashion-resale-data-pipeline/src directory to generate the report results.

Used black formatting.

## Roadmap

_One known bug_: need to run the main file through fashion-data-pipeline/src directory or else it will fail getting raw data files

_Improvements_: with more time I would move the majority of the processing out of the main file and handle that within a 'ResaleProcessor' class of sorts, depending on needs and intended usage

## Report Results

_Based on the files in data/raw for Djerf Avenue and Oscar De La Renta, these are the results generated from the report_

Djerf Avenue sent us 1000 items

725 items had valid data

275 items had invalid data, with a 72.5% rate of data completeness

304 items had the desired predicted profit margin of $20, with a 41.93% rate of resale eligibility among valid items


Oscar De La Renta sent us 795 items

575 items had valid data

220 items had invalid data, with a 72.33% rate of data completeness

471 items had the desired predicted profit margin of $20, with a 81.91% rate of resale eligibility among valid items
