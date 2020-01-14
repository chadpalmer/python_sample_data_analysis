from statistics import mean

class MathUtil:

    def __init__(self):
        pass

    @staticmethod
    def get_max_value_list_index(value_list, value_index):
        max_value = -999999999999
        return_list = []
        for list_row in value_list:
            test_value = list_row[value_index]
            if test_value > max_value:
                max_value = test_value
                return_list = list_row

        return return_list

    @staticmethod
    def get_min_value_list_index(value_list, value_index):
        min_value = 999999999999
        return_list = []
        for list_row in value_list:
            test_value = list_row[value_index]
            if test_value < min_value:
                min_value = test_value
                return_list = list_row

        return return_list

    @staticmethod
    def get_average_list_index(value_list, value_index):
        added_total = 0
        for list_row in value_list:
            added_total += list_row[value_index]

        return added_total / len(value_list)

    @staticmethod
    def get_average_of_list(value_list):
        return mean(value_list)