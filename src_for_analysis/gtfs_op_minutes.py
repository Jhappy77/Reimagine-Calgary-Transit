import pandas as pd
import os
import zipfile
from datetime import datetime, timedelta

############### PARAMETERS. MUST BE SET TO RUN NEW DATASET.

# dataset = "calgary_transit_sep_5_2019"
# dataset = "calgary_transit_nov_2019"
# dataset = "calgary_transit_jan_24_2020"
# dataset = "calgary_transit_jan_27_2021"
# dataset = "calgary_transit_jul_27_2022"
dataset = "calgary_transit_dec_6_2022"

# True: this is the first time running this dataset. 
# False: isn't. Saves time
first_run = False

# MUST ADJUST THESE FOR EACH RUN.
# Check Calendar from GTFS
# Pick from 1 date range. Regular weekdays: 1,1,1,1,1,0,0. Weekdays except Friday: 1,1,1,1,0,0,0

# Sep 5 2019
# regular_weekday_service_id = '2019SE-1BUSWKa-Weekday-05'
# weekday_except_friday_service_id = '2019SE-1BUSWKa-Weekday-05-1111000'
# Nov 2019
# regular_weekday_service_id = '2019SE-1BUSWK-Weekday-28'
# weekday_except_friday_service_id = '2019SE-1BUSWK-Weekday-28-1111000'
# Jan 24 2020
# regular_weekday_service_id = '2019DE-1BUSWK-Weekday-01'
# weekday_except_friday_service_id = '2019DE-1BUSWK-Weekday-01-1111000'
# Jan 27 2021
# regular_weekday_service_id = '2020DE-1BUSWK-Weekday-02'
# weekday_except_friday_service_id = '2020DE-1BUSWK-Weekday-02-1111000'
# Jul 27 2022
# regular_weekday_service_id = '2022JU-1BUSWK-Weekday-02'
# weekday_except_friday_service_id = 'None'
# Sept 21 2022
# regular_weekday_service_id = '2022SE-1BUSWK-Weekday-03'
# weekday_except_friday_service_id = '2022SE-1BUSWK-Weekday-03-1111000'
# Dec 6 2022
regular_weekday_service_id = '2022DE-1BUSWK-Weekday-03'


############### END OF PARAMETERS


zip_data_dir = f'datasets/{dataset}.zip'
output_dir = f'output/{dataset}'
z_data = zipfile.ZipFile(zip_data_dir)
if first_run:
    os.mkdir(output_dir)

FMT = '%H:%M:%S'
def get_time(tstring: str) -> datetime:
    try:
        return datetime.strptime(tstring, FMT)
    except ValueError:
        h, m, s = tstring.split(':')
        h = int(h)
        if h > 23:
            h -= 24
            return get_time(f'{h}:{m}:{s}')
        raise Exception(f'Unexpected error inside function get_time with time {tstring}')

# Also saves them in dataset folder
def get_trip_times_new()-> pd.DataFrame:
    stop_times_df = pd.read_csv(z_data.open('stop_times.txt'))
    grouped_stop_times = stop_times_df.groupby('trip_id')
    trip_time_columns = ['trip_id', 'duration (mins)', 'start_time', 'end_time', 'first_stop_id', 'last_stop_id']
    trip_times = pd.DataFrame(None, None, columns=trip_time_columns)
    for (trip_id, group_df) in grouped_stop_times:
        first_stop = group_df.iloc[0]
        last_stop = group_df.iloc[-1]
        ftime, ltime = first_stop['arrival_time'], last_stop['arrival_time']

        fdtime, ldtime = get_time(ftime), get_time(ltime)
        tdelta = ldtime - fdtime
        if tdelta.days < 0:
            # print(f'Negative days: {trip_id} - {ftime},  {ltime} ')
            tdelta = timedelta(
                days=0,
                seconds=tdelta.seconds,
                microseconds=tdelta.microseconds
            )
        duration = tdelta.total_seconds()/60
        trip_row = pd.Series([trip_id, duration, ftime, ltime, first_stop['stop_id'], last_stop['stop_id']], index=trip_time_columns)
        row_df = pd.DataFrame([trip_row], index=[trip_id], columns=trip_time_columns)
        trip_times = pd.concat([trip_times, row_df], ignore_index=True)

    trip_times.to_csv(f'{output_dir}/trip_times.txt', index=False)

    return trip_times

# For trip times which have already been generated into output folder
def get_trip_times_old(output_dir: str)->pd.DataFrame:
    return pd.read_csv(f'{output_dir}/trip_times.txt')

def calc_headways_by_hour(df: pd.DataFrame, combined: bool):
    df['end_hr'] = df.apply(lambda row: row.end_time.split(':')[0], axis=1)
    grouped: pd.DataFrameGroupBy
    if combined:
        grouped = df.groupby(['route_id', 'end_hr'])
    else:
        grouped = df.groupby(['route_id', 'end_hr', 'trip_headsign'])
    buses_by_hour = grouped['trip_id'].count().to_frame()
    buses_by_hour.rename(columns={'trip_id': 'buses per hour'}, inplace=True)
    buses_by_hour['headway by hour'] = buses_by_hour.apply(lambda row: 60/row['buses per hour'], axis=1)
    if combined:
        buses_by_hour.to_csv(f'{output_dir}/headway_by_hour_combined.txt')
    else:
        buses_by_hour.to_csv(f'{output_dir}/headway_by_hour_split.txt')


# Toggle between new and old as necessary. Will need to run new first time. But running new is expensive so should run old on any subsequent runs.
trip_times: pd.DataFrame
if first_run:
    trip_times = get_trip_times_new()
else:
    trip_times = get_trip_times_old(output_dir)

trips = pd.read_csv(z_data.open('trips.txt'))
trip_times_with_trip_info = trip_times.merge(trips, on='trip_id')
trip_times_with_trip_info.sort_values(by=['route_id', 'start_time'], inplace=True)

indexes_matching_service_id = trip_times_with_trip_info[trip_times_with_trip_info['service_id'] == regular_weekday_service_id].index
# indexes_matching_weekday_sans_friday = trip_times_with_trip_info[trip_times_with_trip_info['service_id'] == weekday_except_friday_service_id].index
# matching_indexes = indexes_matching_service_id.union(indexes_matching_weekday_sans_friday)
indexes_that_dont_match = trip_times_with_trip_info.index.difference(indexes_matching_service_id)
trip_times_with_trip_info.drop(indexes_that_dont_match, inplace=True)

trip_times_with_trip_info.to_csv(f'{output_dir}/trip_times_with_trip_info.txt', index=False)

calc_headways_by_hour(trip_times_with_trip_info, True)
calc_headways_by_hour(trip_times_with_trip_info, False)

grouped_by_route_id = trip_times_with_trip_info.groupby('route_id')
number_buses = grouped_by_route_id.size()
route_op_minutes: pd.DataFrame = grouped_by_route_id.agg(operating_mins=('duration (mins)', 'sum'))   #['duration (mins)'].sum().to_frame()
op_info = route_op_minutes.merge(number_buses.rename('trip_count'), on='route_id')

routes = pd.read_csv(z_data.open('routes.txt'))
route_op_minutes_w_route_info = op_info.merge(routes, on='route_id')
route_op_minutes_w_route_info['route_short_name'] = pd.to_numeric(route_op_minutes_w_route_info['route_short_name'])
dropped_above500 = route_op_minutes_w_route_info.drop(route_op_minutes_w_route_info[route_op_minutes_w_route_info['route_short_name'] >= 500].index)
sorted_route_op_mins = dropped_above500.sort_values(by='route_short_name')
sorted_route_op_mins.to_csv(f'{output_dir}/route_operating_minutes.txt', index=False)

print(f'Successfully processed files in {dataset} and outputted to {output_dir}')