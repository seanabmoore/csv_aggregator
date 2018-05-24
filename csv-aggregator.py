#!/usr/bin/env python

import datetime
import csv


class CSVAggregator:
    """
        CSVAggregator, aggregates the rows of a csv file and performs a groupby on a specified tuple.
        Assumption is that the first
    """
    __AGGREGATE_FIELDS = ['Count', 'Sum']

    def __init__(self, input_file_location, output_file_location):
        self._file_reader = open(input_file_location, "r")
        self._file_writer = open(output_file_location, "w")

    @staticmethod
    def get_record_stats(record, header_row, fields, aggregate_field, acc):
        agg = float(record[header_row.index(aggregate_field)])
        tup = tuple(record[header_row.index(i)] for i in fields)
        if acc.get(tup):
            acc[tup]["sum"] += agg
            acc[tup]["count"] += 1
        else:
            acc[tup] = dict()
            acc[tup]["sum"] = agg
            acc[tup]["count"] = 1
        return acc

    @staticmethod
    def return_date_grouping(date):
        d = datetime.datetime.strptime(date, "'%d-%b-%Y'")
        return d.strftime("'%b %Y'")

    def group_by(self, group_by_fields, aggregate_field):
        acc = dict()
        reader = csv.reader(self._file_reader, delimiter=',')
        fields = next(reader, None)
        for record in reader:
            if "Date" in fields:
                record[fields.index("Date")] = self.return_date_grouping(record[fields.index("Date")])
            acc = self.get_record_stats(record, fields, group_by_fields, aggregate_field, acc)
        self._file_reader.close()
        return acc

    def aggregate_to_file(self, group_by_fields,aggregate_field):
        writer = csv.writer(self._file_writer, delimiter=',')
        writer.writerow(group_by_fields + self.__AGGREGATE_FIELDS)
        output = self.group_by(group_by_fields, aggregate_field)
        for key, value in output.items():
            writer.writerow(key + tuple(value.values()))

    def close_files(self):
        if self._file_reader:
            self._file_reader.close()
        if self._file_writer:
            self._file_writer.close()


if __name__ == '__main__':
    group_by_fields = ['Product', 'Network', 'Date']
    aggregate_field = 'Amount'
    aggregator = CSVAggregator(input_file_location="hello.csv", output_file_location="test.csv")
    aggregator.aggregate_to_file(group_by_fields, aggregate_field)
