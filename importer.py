import sys
from connect_db import ConnectDB
from open_files import OpenFiles
from date_time_util import DateTimeUtil


def do_import():
    print("Importing data. Please wait...")
    db_connection = ConnectDB.connect_db(sys.argv[1])
    if db_connection is not None:
        bln_load_imported = import_load(db_connection)
        if bln_load_imported:
            bln_weather_imported = import_weather_obs(db_connection)
            if bln_weather_imported:
                print("Import successful. Both system load and weather obs data have been imported successfully.")
            else:
                print("Import failed. Data table for weather observations could not be created.")
        else:
            print("Import failed. Data table for system load could not be created.")

        clean_up(db_connection)
    else:
        print("Import failed. Database connection could not be established.")


def import_load(db_connection):
    if db_connection is not None:
        # First, lets get the system load tables imported
        # Start by creating DB table for system load
        str_sql_table_command = '''CREATE TABLE IF NOT EXISTS system_load (
                                        id integer PRIMARY KEY,
                                        OperDay text,
                                        HourEnding text,
                                        COAST real,
                                        EAST real,
                                        FAR_WEST real,
                                        NORTH real,
                                        NORTH_C real,
                                        SOUTHERN real,
                                        SOUTH_C real,
                                        WEST real,
                                        TOTAL real,
                                        DSTFlag text,
                                        day_id integer,
                                        hour_id integer,
                                        year integer
                                    );'''
        bln_table_created = ConnectDB.create_table(db_connection, str_sql_table_command)
        if bln_table_created:
            csv_list = OpenFiles.get_file_list("./system_load_by_weather_zone", "csv")
            # Now, let's loop through all the files and add the info to the new system_load DB table.
            for csv_file in csv_list:
                row_list = OpenFiles.open_csv_file(csv_file, ",")
                for index, row in enumerate(row_list):
                    if index != 0:
                        row_date = DateTimeUtil.string_to_date(row[0], "America/Chicago")

                        # Calculating and adding day_id and hour_id to row list so that I can tie together the weather
                        # data table to the system load data table
                        row.append(DateTimeUtil.get_day_of_year(row_date))
                        time_list = row[1].split(":")
                        row.append(int(time_list[0]))
                        row.append(row_date.year)
                        for i in range(2, 11):
                            row[i] = float(row[i])

                        # Now let's insert the data row into the system_load DB table
                        str_sql_row_command = '''INSERT INTO system_load(OperDay,HourEnding,COAST,EAST,FAR_WEST,NORTH,NORTH_C,SOUTHERN,SOUTH_C,WEST,TOTAL,DSTFlag,day_id,hour_id,year) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                        row_id = ConnectDB.insert_row_into_table(db_connection, str_sql_row_command, tuple(row))
            return True
        else:
            return False


def import_weather_obs(db_connection):
    # Now let us tackle the weather observation tables for the various stations
    str_sql_table_command = '''CREATE TABLE IF NOT EXISTS weather_obs (
                                    id integer PRIMARY KEY,
                                    TimeCST text,
                                    temperature real,
                                    dewpoint text,
                                    humidity text,
                                    sea_level_pressure text,
                                    visibility text,
                                    wind_direction text,
                                    wind_speed text,
                                    gust_speed text,
                                    precip text,
                                    events text,
                                    conditions text,
                                    wind_dir_degrees text,
                                    date_utc text,
                                    location text,
                                    day_id integer,
                                    hour_id integer,
                                    year integer
                                );'''
    bln_table_created = ConnectDB.create_table(db_connection, str_sql_table_command)
    if bln_table_created:
        location_tuple = ("KHOU", "KDAL", "KSAT")
        year_tuple = ("2014", "2015")
        for loc_index, location in enumerate(location_tuple):
            for year_index, year in enumerate(year_tuple):
                csv_list = OpenFiles.get_file_list("./weather_data/%s_%s" % (location, year), "csv")
                for csv_file in csv_list:
                    row_list = OpenFiles.open_csv_file(csv_file, ",")
                    # only want to have one hour_id for each hour so some rows will be dropped.
                    hour_id_list = []
                    for index, row in enumerate(row_list):
                        if index != 0:
                            # The time in these files are a mess so I will round to nearest hour and eliminate the
                            # extra observations so that the weather data maps well with the system load data.
                            init_time_split = row[0].split(" ")
                            time_list = init_time_split[0].split(":")
                            hour_id = 0
                            if (int(time_list[0]) == 12) and (int(time_list[1]) > 30) and (init_time_split[1] == "AM") and (1 not in hour_id_list):
                                hour_id = 1
                                hour_id_list.append(hour_id)
                            elif (int(time_list[0]) < 12) and (int(time_list[1]) > 30) and (init_time_split[1] == "AM") and (int(time_list[0]) + 1 not in hour_id_list):
                                hour_id = int(time_list[0]) + 1
                                hour_id_list.append(hour_id)
                            elif (int(time_list[0]) == 12) and (int(time_list[1]) > 30) and (init_time_split[1] == "PM") and (13 not in hour_id_list):
                                hour_id = 13
                                hour_id_list.append(hour_id)
                            elif (int(time_list[0]) < 12) and (int(time_list[1]) > 30) and (init_time_split[1] == "PM") and (int(time_list[0]) + 13 not in hour_id_list):
                                hour_id = int(time_list[0]) + 13
                                hour_id_list.append(hour_id)

                            if hour_id != 0:
                                # Adding location and calculating and adding day_id and hour_id to row list so that I
                                # can tie together the weather data table to the system load data table
                                row.append(location)
                                row_date = DateTimeUtil.string_to_date(row[13], "UTC")
                                row_date = DateTimeUtil.UTC_to_local(row_date, "America/Chicago")
                                row.append(DateTimeUtil.get_day_of_year(row_date))
                                row.append(hour_id)
                                row.append(row_date.year)
                                row[1] = float(row[1])

                                # Now let's insert the data row into the weather_obs DB table
                                str_sql_row_command = '''INSERT INTO weather_obs(TimeCST,temperature,dewpoint,humidity,sea_level_pressure,visibility,wind_direction,wind_speed,gust_speed,precip,events,conditions,wind_dir_degrees,date_utc,location,day_id,hour_id,year) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                                row_id = ConnectDB.insert_row_into_table(db_connection, str_sql_row_command, tuple(row))
                                # print(row)
        return True
    else:
        return False


def clean_up(db_connection):
    ConnectDB.close_connection(db_connection)


do_import()
