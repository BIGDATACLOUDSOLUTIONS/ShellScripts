import sys
import os
import re
from datetime import datetime

# date_time_format = %Y-%m-%d %H:%M:%S
date_time_format = 19

"""
Example #1
Input: 2017-10-19 13:23:39.00
Output: 2017-10-19 13:23:39.000000

Example #2
Input: 2017-10-19 13:23:39
Output: 2017-10-19 13:23:39.000000

Example #3
Input: 2017-10-19 13:23:39.0034
output: 2017-10-19 13:23:39.003400

Example #4
Input: 2018-08-15:22:15:06.112000
Output: 2018-08-15 22:15:06.1120002018

"""


def remove_trailing_space(data):
    for (loc, item) in enumerate(data):
        data[loc] = item.strip()
    return data


def reformat_date(date):
    if len(date) > date_time_format:
        if date[10] == ':':
            curr_fmt = "%Y-%m-%d:%H:%M:%S.%f"
        else:
            curr_fmt = "%Y-%m-%d %H:%M:%S.%f"
    else:
        curr_fmt = "%Y-%m-%d %H:%M:%S"
    to_fmt = "%Y-%m-%d %H:%M:%S.%f"
    formatted_date = datetime.strptime(date, curr_fmt)
    reformatted_date = datetime.strftime(formatted_date, to_fmt)
    return reformatted_date


def remove_special_characters(data):
    for (loc, item) in enumerate(data):
        data[loc] = " ".join(item.split("\\n"))
    return data


def get_original_string(data):
    return '\007'.join(data)


def transform_file(data, write_file):
    for line in data:
        line_data = line.split('\x07')
        r = re.compile(r'\d+-\d+-\d+[\s+:]\d+:\d+:\d+')
        dates = list(filter(r.match, line_data))
        for date in dates:
            date_index = line_data.index(date)
            line_data[date_index] = reformat_date(date)
        line_data = remove_trailing_space(line_data)
        #line_data = remove_special_characters(line_data)
        transformed_data = get_original_string(line_data)
        write_to_file(write_file, transformed_data)


def read_file(read_file_name):
    data = open(read_file_name, "r")
    file_data = data.read().split('\n')
    data.close()
    return file_data


def write_to_file(write_file, transformed_data):
    write_file.write(transformed_data)
    write_file.write("\n")


def main():
    read_file_name = sys.argv[1]
    write_file_name = read_file_name + "_tmp"
    write_file = open(write_file_name, "w")
    data = read_file(read_file_name)
    transform_file(data, write_file)
    write_file.close()
    os.remove(read_file_name)
    os.rename(read_file_name + "_tmp", read_file_name)


if __name__ == '__main__':
    main()
