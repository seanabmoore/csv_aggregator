# csv_aggregator

Performs a GROUP BY on table data contained in a CSV

## Getting Started


## Prerequisites

Python 3.6

## Testing

The csv input and output files are passed in as command line arguments.
To test this, run

```

python csv_aggregator.py loans.csv output.csv


```

The GROUP_BY tuple can be changed by adding/removing fields from the following in csv_aggregator.py

```python

group_by_fields = ['Product', 'Network', 'Date']

```


## Considerations

* CSVAggregator, aggregates the rows of a csv file and performs a group by on a specified tuple.
* Assumption is that the first row of csv contains column headings

* The CSVAggregator object is initialized with the input and output file name.
* A group by  can be performed by calling the group_by method of the object.
* This takes in the columns that must by grouped ( a tuple) as well as a single column to perform COUNT and SUM on.

## Limitations

* CSVAggregator can handle most groupby column names, however, to handle date, the heading must be specified as Date.
* The group_by clause will then group the date by month and year.
* group_by can only aggregate one field and only performs SUM and COUNT on that field, thus this field must be of type float or int.
* The CSV Files must also be , delimited

## Performance and Scaling
* At present the function only performs single core processing on the data.

## Authors

* Sean Moore
