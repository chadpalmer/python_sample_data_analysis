import sys
from connect_db import ConnectDB
from math_util import MathUtil
from open_files import OpenFiles
import itertools


def create_model():
    print("Creating model and analyzing data. Please wait...")
    db_connection = ConnectDB.connect_db(sys.argv[1])
    if db_connection is not None:
        daily_max_list = get_daily_maxes(db_connection)
        if len(daily_max_list) > 0:
            lowest_max_load_list = MathUtil.get_min_value_list_index(daily_max_list, 1)
            highest_temp_list = MathUtil.get_max_value_list_index(daily_max_list, 4)
            lowest_temp_list = MathUtil.get_min_value_list_index(daily_max_list, 4)

            # This temp and load is assumed to be the level where minimal AC and heat are used thus forming the baseline load and temperature.
            baseline_load = lowest_max_load_list[1]
            baseline_temp = lowest_max_load_list[4]
            # Now we want to calculate the factor for temps lower and higher than the baseline by dividing the difference
            # in loads between the max temp and baseline temp and the min temp and baseline by the difference between both
            # both temps and the baseline temp
            low_temp_factor = abs(lowest_temp_list[1] - lowest_max_load_list[1]) / abs(lowest_max_load_list[4] - lowest_temp_list[4])
            high_temp_factor = abs(highest_temp_list[1] - lowest_max_load_list[1]) / abs(highest_temp_list[4] - lowest_max_load_list[4])

            daily_max_list_2015 = get_daily_maxes_2015(db_connection)
            if len(daily_max_list_2015) > 0:
                final_compare_list = create_comparison(daily_max_list_2015, baseline_temp, low_temp_factor, high_temp_factor, baseline_load)
                str_status = create_output_file(final_compare_list)
                print(str_status)
            else:
                print("Model test failed. Could not get daily load maxes and temperature data for 2015.")
        else:
            print("Model initialization failed. Could not get daily load maxes and temperature data for 2014.")

        clean_up(db_connection)
    else:
        print("Model initialization failed.  Could not connect to DB.")


def get_daily_maxes(db_connection):
    daily_max_list = []
    for i in range(1, 366):
        bln_day_missing = False
        str_get_load_command = "SELECT OperDay,%s,day_id,hour_id FROM system_load WHERE day_id=%s AND year=2014" % (
            sys.argv[3], i)
        query_rows = ConnectDB.select_rows_by_query(db_connection, str_get_load_command)
        # if entire day is missing, just skip
        if len(query_rows) > 0:
            day_max_hour_list = list(MathUtil.get_max_value_list_index(query_rows, 1))
            str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2014 AND day_id=%s AND hour_id=%s" % (
                sys.argv[2], day_max_hour_list[2], day_max_hour_list[3])
            query_temp = ConnectDB.select_rows_by_query(db_connection, str_get_temperature_command)
            try:
                temp = query_temp[0][0]
            except IndexError:
                # If weather data for an hour is missing. Take the average of the previous hour and the next hour. If the hour is 1 we will simply use 2. If the hour is 24, we will use 23.
                if day_max_hour_list[3] == 1:
                    str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2014 AND day_id=%s AND hour_id=%s" % (
                        sys.argv[2], day_max_hour_list[2], day_max_hour_list[3] + 1)
                elif day_max_hour_list[3] == 24:
                    str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2014 AND day_id=%s AND hour_id=%s" % (
                        sys.argv[2], day_max_hour_list[2], day_max_hour_list[3] - 1)
                else:
                    str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2014 AND day_id=%s AND hour_id BETWEEN %s AND %s" % (
                        sys.argv[2], day_max_hour_list[2], day_max_hour_list[3] - 1, day_max_hour_list[3] + 1)

                query_temp_average = ConnectDB.select_rows_by_query(db_connection, str_get_temperature_command)
                if len(query_temp_average) > 0:
                    temp = MathUtil.get_average_of_list(list(itertools.chain(*query_temp_average)))
                else:
                    # If entire day is missing, just skip
                    bln_day_missing = True
                    temp = 0

            if not bln_day_missing:
                day_max_hour_list.append(temp)
                daily_max_list.append(day_max_hour_list)

    return daily_max_list


def get_daily_maxes_2015(db_connection):
    # Now we will get actual peak load data from 2015 and use the model to forecast the 2015 peak load using the
    # observed temperature and compare the forecast with the actual load.
    daily_max_list_2015 = []
    for i in range(1, 366):
        bln_day_missing = False
        str_get_load_command = "SELECT OperDay,%s,day_id,hour_id FROM system_load WHERE day_id=%s AND year=2015" % (
            sys.argv[3], i)
        query_rows = ConnectDB.select_rows_by_query(db_connection, str_get_load_command)
        # if entire day is missing, just skip
        if len(query_rows) > 0:
            day_max_hour_list = list(MathUtil.get_max_value_list_index(query_rows, 1))
            str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2015 AND day_id=%s AND hour_id=%s" % (
                sys.argv[2], day_max_hour_list[2], day_max_hour_list[3])
            query_temp = ConnectDB.select_rows_by_query(db_connection, str_get_temperature_command)
            try:
                temp = query_temp[0][0]
            except IndexError:
                # If weather data for an hour is missing. Take the average of the previous hour and the next hour. If the hour is 1 we will simply use 2. If the hour is 24, we will use 23.
                if day_max_hour_list[3] == 1:
                    str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2015 AND day_id=%s AND hour_id=%s" % (
                        sys.argv[2], day_max_hour_list[2], day_max_hour_list[3] + 1)
                elif day_max_hour_list[3] == 24:
                    str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2015 AND day_id=%s AND hour_id=%s" % (
                        sys.argv[2], day_max_hour_list[2], day_max_hour_list[3] - 1)
                else:
                    str_get_temperature_command = "SELECT temperature FROM weather_obs WHERE location LIKE '%s' AND year=2015 AND day_id=%s AND hour_id BETWEEN %s AND %s" % (
                        sys.argv[2], day_max_hour_list[2], day_max_hour_list[3] - 1, day_max_hour_list[3] + 1)

                query_temp_average = ConnectDB.select_rows_by_query(db_connection, str_get_temperature_command)
                if len(query_temp_average) > 0:
                    temp = MathUtil.get_average_of_list(list(itertools.chain(*query_temp_average)))
                else:
                    # If entire day is missing, just skip
                    bln_day_missing = True
                    temp = 0

            if not bln_day_missing:
                day_max_hour_list.append(temp)
                daily_max_list_2015.append(day_max_hour_list)

    return daily_max_list_2015


def create_comparison(daily_max_list_2015, baseline_temp, low_temp_factor, high_temp_factor, baseline_load):
    # Now we will predict the peak load and compare it to the actual peak load
    final_compare_list = [["Date", "Forecasted_Peak", "Actual_Peak"]]
    for row in daily_max_list_2015:
        if row[4] <= baseline_temp:
            forecast_peak = round(((baseline_temp - row[4]) * low_temp_factor) + baseline_load, 2)
        else:
            forecast_peak = round(((row[4] - baseline_temp) * high_temp_factor) + baseline_load, 2)

        temp_list = [row[0], forecast_peak, row[1]]
        final_compare_list.append(temp_list)

    return final_compare_list


def create_output_file(final_compare_list):
    # Finally, we create the output csv files using the final comparison list.
    str_file = OpenFiles.create_csv_file("./forecast_output_%s_%s.csv" % (sys.argv[2], sys.argv[3]), ",", final_compare_list)
    return "Forecast output file (%s) has been created." % str_file


def clean_up(db_connection):
    ConnectDB.close_connection(db_connection)


create_model()
