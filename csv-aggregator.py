#!/usr/bin/env python

import datetime
import csv
import sys
import logging

# Create Logger
logger = logging.getLogger('csv_aggregator')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("aggregator.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class CSVAggregator:
    """
        CSVAggregator, aggregates the rows of a csv file and performs a group by on a specified tuple.
        Assumption is that the first row of csv contains column headings

        Limitations:
            CSVAggregator can handle most group by headings, however, to handle date, the heading must be specified
            as Date. The group by clause will then group the date by month and year.
            Group by can only aggregate one field and only performs SUM and COUNT on that field, thus this field must be
            of type float or int.

            The CSV Files must also be , delimited

    """
    __AGGREGATE_FIELDS = ['Sum', 'Count']

    def __init__(self, input_file_location, output_file_location):
        try:
            self._file_reader = open(input_file_location, "r")
            self._file_writer = open(output_file_location, "w")
        except Exception as e:
            logger.info("Error opening files")
            logger.exception("Exception Message")
            raise e

    @staticmethod
    def aggregate(record, header_row, fields, agg_field, acc):
        agg = float(record[header_row.index(agg_field)])
        tup = tuple(record[header_row.index(i)] for i in fields)
        if acc.get(tup):
            acc[tup]["sum"] += agg
            acc[tup]["count"] += 1
        else:
            acc[tup] = dict()
            acc[tup]["sum"] = agg
            acc[tup]["count"] = 1
        return acc

    '''
        Returns Date in the Month, Year format
    '''

    @staticmethod
    def return_date_grouping(date):
        d = datetime.datetime.strptime(date, "'%d-%b-%Y'")
        return d.strftime("'%b %Y'")

    def group_by(self, group_by_args, aggregate_arg):
        acc = dict()
        reader = csv.reader(self._file_reader, delimiter=',')
        fields = [x.lower().strip() for x in next(reader, None)]
        group_by_args = [x.lower().strip() for x in group_by_args]
        aggregate_arg = aggregate_arg.strip().lower()
        for record in reader:
            # There is a special consideration for the date field in the record.
            if "date" in fields:
                record[fields.index("date")] = self.return_date_grouping(record[fields.index("date")])
            acc = self.aggregate(record, fields, group_by_args, aggregate_arg, acc)
        self._file_reader.close()
        return acc

    def aggregate_to_file(self, group_by_arg, aggregate_arg):
        writer = csv.writer(self._file_writer, delimiter=',')
        writer.writerow(group_by_arg + self.__AGGREGATE_FIELDS)
        output = self.group_by(group_by_arg, aggregate_arg)

        for key, value in output.items():
            writer.writerow(key + (value["sum"], value["count"]))
        self._file_writer.close()

    def close_files(self):
        if self._file_reader:
            self._file_reader.close()
        if self._file_writer:
            self._file_writer.close()


if __name__ == '__main__':
    group_by_fields = ['Product', 'Network', 'Date']
    aggregate_field = 'Amount'
    try:
        input_file_location = sys.argv[1]
        output_file_location = sys.argv[2]
        logger.info(
            "Starting CSV Aggregator, with input file: " + input_file_location + " and output file: " + output_file_location)
        aggregator = CSVAggregator(input_file_location, output_file_location)
        aggregator.aggregate_to_file(group_by_fields, aggregate_field)
        logger.info("Finished job")
        aggregator.close_files()
    except Exception as e:
        logger.info("Error with file")
        logger.exception("Exception Message")
