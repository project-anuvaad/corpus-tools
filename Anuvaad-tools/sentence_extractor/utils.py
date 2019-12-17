import sentence_extractor.extractor_constants as CONSTANTS
import csv


def write_to_csv(data, processId, header, path):
    filename = processId + header + '.csv'
    complete_file_path = path + processId + '/' + filename
    with open(complete_file_path, CONSTANTS.CSV_WRITE) as file:
        writer = csv.writer(file)
        for line in data:
            writer.writerow([line])
        file.close()
    return filename
