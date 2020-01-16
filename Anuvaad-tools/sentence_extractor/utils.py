import utils.anuvaad_constants as CONSTANTS
import csv
import sys
maxInt = sys.maxsize
csv.field_size_limit(maxInt)


def write_to_csv(data, processId, header, path, workspace):
    filename = workspace + '_' + header + '_' + processId + '.csv'
    complete_file_path = path + processId + '/' + filename
    with open(complete_file_path, CONSTANTS.CSV_WRITE) as file:
        writer = csv.writer(file)
        for line in data:
            writer.writerow([line])
        file.close()
    return filename


def read_from_csv(filepath):
    file_data = []
    with open(filepath, CONSTANTS.CSV_RT) as file:
        data = csv.reader(file)
        for row in data:
            text = row[0]
            file_data.append(text)
        file.close()
    return file_data