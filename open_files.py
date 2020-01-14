import os
import csv


class OpenFiles:

    def __init__(self):
        pass

    @staticmethod
    def get_file_list(str_folder=".", str_ext=""):
        file_list = []
        for file in os.listdir(str_folder):
            if str_folder != "":
                if file.endswith(".%s" % str_ext):
                    file_list.append(os.path.join(str_folder, file))
            else:
                file_list.append(os.path.join(str_folder, file))

        return file_list

    @staticmethod
    def open_csv_file(str_path, str_delimiter):
        with open(str_path, "r") as csvfile:
            data_reader = csv.reader(csvfile, delimiter=str_delimiter, quotechar='|')
            row_list = []
            for row in data_reader:
                row_list.append(row)

        return row_list

    @staticmethod
    def create_csv_file(str_path, str_delimiter, row_list):
        with open(str_path, "w", newline="") as csvfile:
            file_writer = csv.writer(csvfile, delimiter=str_delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in row_list:
                file_writer.writerow(row)

        return str_path
