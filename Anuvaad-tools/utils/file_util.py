import csv
import utils.anuvaad_constants as Constants


def write_to_csv(filepath, data, mode=Constants.CSV_APPEND, rows=2):
    sentence_count = 0
    unique = set()
    with open(filepath, mode, encoding='utf-8', errors="ignore") as file:
        writer = csv.writer(file)
        for line in data:
            if rows == 1:
                if not unique.__contains__(line):
                    writer.writerow([line])
                    sentence_count = sentence_count + 1
            elif rows == 2:
                if not unique.__contains__(line[Constants.SOURCE]):
                    unique.add(line[Constants.SOURCE])
                    sentence_count = sentence_count + 1
                    writer.writerow([line[Constants.SOURCE], line[Constants.TARGET]])

    return sentence_count


def read_csv(filepath, rows=1):
    unique = list()
    with open(filepath, Constants.CSV_RT) as source:
        reader = csv.reader(source)
        for line in reader:
            if rows == 1:
                unique.append(line[0])
            if rows == 2:
                data = {Constants.SOURCE: line[0], Constants.TARGET: line[1]}
                unique.append(data)
        source.close()
    return unique

