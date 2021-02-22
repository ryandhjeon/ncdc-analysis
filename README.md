# Data analysis with MapReduce

__National Oceanic and Atmospheric Administration's (NOAA) National Climatic Data Center (NCDC)__

Data is available at [NOAA(Link)](ftp://ftp.ncdc.noaa.gov/pub/data/noaa/)

---
> The commands used are in the `commands.txt` file. 

## Task #0: Filtering

> We are only interested in specific weather stations that were operable (aka, reported at least 2 observations every month during a given year). If a particular weather station was not operable in a particular year, all of its data (if any) for that year should be filtered out. Keep the years where it was operable! All analysis tasks will operate on this subset of data, so be sure to save it into HDFS in a format usable as input in later tasks!

### Step 1 
__Mapper 1:__ The goal was to filter out the necessary information from the original data for the future usage. As it goes through the original data line by line, values were extracted from each line. The keys of id, years, and month with the values of temperatures per line, were mapped for the reducer 1.

__Reducer 1:__ The task was to pass on the data only if 2 or more observations existed. After counting how many temperatures each station measured, only lines with 2 or more stations were passed onto next reducer. 

### Step 2
__Reducer 2:__ As a final reducer, if the length of the pairs was equal to 12, as there are twelve months in a year, the 'id' was emitted as a key, and 'id, year, month, temperature' was emitted as values. The 'id' was included in the again in values as it made easier for playing with data in the future.

## Task #1: Analysis

> If any stations were operable all years in that period, generate a report of their station IDs.
If none were operable for all years in that period, rank the stations by which station was operable the greatest number of years and list the top 50 stations. 

### Step 1
__Mapper 1:__ The output from task00 was received. The values were again extracted, and yielded `id` and `year` only if the year was bigger than 1920, and less than 1940.

__Reducer 1:__ The years mapped from the mapper were gathered to create the list. Then, by calculating the difference of year 1941 to 1920, and multiplying 0.8, it was possible to find the 80% of operated stations.
(by the greatest number of years operable), and include what years each station was operable. The first if statement emitted the ones that were operating 80% or more from 1920 to 1940. The second if statement emitted the stations that were operated in all years between 1920 to 1940.

### Step 2
__Reducer 2:__ The final reducer emitted the top 50 stations operated if non were operable for all year in the period as instructed.

> What (if any) weather stations were operable for 80% or more of the entire time period 1920-1940?
> 
> Yes. Top 50 station list:
> 108650, 103380, 29750, 29110, 106370, 104190, 267020, 103840, 101270, 101200, 228020, 124250, 124000, 122050, 121160, 121140, 116430, 115180, 112310, 111200, 110350, 109350, 108660, 107760, 107630, 107280, 107270, 106850, 105780, 105770, 105540, 105130, 105010, 104880, 104690, 104680, 104530, 104270, 104100, 103610, 101700, 101470, 101310, 100910, 100670, 100190, 28360, 333930, 330190, 265090


## Task #2: More Analysis

## Task #3: Analysis

## Task #4: Analysis