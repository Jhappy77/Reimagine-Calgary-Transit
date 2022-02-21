import pandas as pd
from datetime import datetime, timedelta

# dataset = "calgary_transit_nov_2019"
dataset = "calgary_transit_jan_24_2020"
directory = f'src/datasets/{dataset}'
output_dir = f'src/output/{dataset}'


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
def get_trip_times_new(directory: str)-> pd.DataFrame:
    stop_times_df = pd.read_csv(f'{directory}/stop_times.txt')
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

# For trip times which have already been generated into directory folder
def get_trip_times_old(output_dir: str)->pd.DataFrame:
    return pd.read_csv(f'{output_dir}/trip_times.txt')

# trip_times = get_trip_times_new(directory)
trip_times = get_trip_times_old(output_dir)
# print(new_df.iloc[0])
# print("generated:")
# print(gen_df.iloc[0])

trips = pd.read_csv(f'{directory}/trips.txt')
trip_times_with_trip_info = trip_times.merge(trips, on='trip_id')
trip_times_with_trip_info.sort_values(by=['route_id', 'start_time'], inplace=True)
#regular_weekday_service_ids = ['2019SE-1BUSWK-Weekday-28']
regular_weekday_service_id = '2019DE-1BUSWK-Weekday-01'
weekday_except_friday_service_id = '2019DE-1BUSWK-Weekday-01-1111000'
indexes_matching_service_id = trip_times_with_trip_info[trip_times_with_trip_info['service_id'] == regular_weekday_service_id].index
indexes_matching_weekday_sans_friday = trip_times_with_trip_info[trip_times_with_trip_info['service_id'] == weekday_except_friday_service_id].index
matching_indexes = indexes_matching_service_id.union(indexes_matching_weekday_sans_friday)
indexes_that_dont_match = trip_times_with_trip_info.index.difference(matching_indexes)
trip_times_with_trip_info.drop(indexes_that_dont_match, inplace=True)

trip_times_with_trip_info.to_csv(f'{output_dir}/trip_times_with_trip_info.txt', index=False)


grouped_by_route_id = trip_times_with_trip_info.groupby('route_id')
route_op_minutes: pd.DataFrame = grouped_by_route_id['duration (mins)'].sum().to_frame()
route_op_minutes.rename(columns={'duration (mins)': 'operating mins'}, inplace=True)
routes = pd.read_csv(f'{directory}/routes.txt')
route_op_minutes_w_route_info = route_op_minutes.merge(routes, on='route_id')
route_op_minutes_w_route_info.to_csv(f'{directory}/route_operating_minutes.txt')
route_op_minutes_w_route_info['route_short_name'] = pd.to_numeric(route_op_minutes_w_route_info['route_short_name'])
dropped_above500 = route_op_minutes_w_route_info.drop(route_op_minutes_w_route_info[route_op_minutes_w_route_info['route_short_name'] >= 500].index)
sorted_route_op_mins = dropped_above500.sort_values(by='route_short_name')
sorted_route_op_mins.to_csv(f'{output_dir}/route_operating_minutes.txt', index=False)
sum = sorted_route_op_mins['operating mins'].sum()
print(f'Total operating minutes: {sum}')