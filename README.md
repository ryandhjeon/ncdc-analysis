# Data analysis with MapReduce

__National Oceanic and Atmospheric Administration's (NOAA) National Climatic Data Center (NCDC)__

Data is available at [NOAA(Link)](ftp://ftp.ncdc.noaa.gov/pub/data/noaa/)

---
> The commands used are in the `commands.txt` file. 

## Task #0: Filtering

### Step 1 
__Mapper 1:__ The goal was to filter out the necessary information from the original data for the future usage. As it goes through the original data line by line, values were extracted from each line. The keys of id, years, and month with the values of temperatures per line, were mapped for the reducer 1.

__Reducer 1:__ The task was to pass on the data only if 2 or more observations existed. After counting how many temperatures each station measured, only lines with 2 or more stations were passed onto next reducer. 

### Step 2: 
__Reducer 2:__ As a final reducer, if the length of the pairs was equal to 12, as there are twelve months in a year, the 'id' was emitted as a key, and 'id, year, month, temperature' was emitted as values. The 'id' was included in the again in values as it made easier for playing with data in the future.


## Task #1: Analysis


## Task #2: More Analysis

## Task #3: Analysis

## Task #4: Analysis