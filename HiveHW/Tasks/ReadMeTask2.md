### DataSet
- [Flying](http://stat-computing.org/dataexpo/2009/the-data.html ) (year 2007)
- [Airports](http://stat-computing.org/dataexpo/2009/supplemental-data.html) (Airports)
- [Carriers](http://stat-computing.org/dataexpo/2009/supplemental-data.html) (Carriers)

### Tasks:
0. Find all carriers who cancelled more than 1 flights during 2007, order them from biggest to lowest by number 
of cancelled flights and list in each record all departure cities where cancellation happened. (Screenshot #1)
1. How many MR jobs where instanced for this query?
    
### Acceptance criteria
- Hive used
- Ambari or beeline clients used

### Expected outputs:
1. ZIP-ed folder with your scripts:
    1. queries
    2. table initialization scripts
2. Screenshot #1 with executed queries and result.
3. Answer to question in point 1(How many jobs...)

    #### Result example

    |Carrier| Canceled flights|Cities|
    |-------|:---------------:|----------------------------:|
    |AIR1   |73               |Chicago, New York, San Diego |
    |AIR1   |45               |Miami, Tucson, Huston, Boston|


# Hints:
    - you can use temporary tables to solve this task