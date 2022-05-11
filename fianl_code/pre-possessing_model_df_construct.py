import pandas as pd
import datetime
london = pd.read_csv("london.csv")
# check and drop na
sum(london.isna().any(1))
london = london.dropna()

# change date_time column into datetime data type
london['start_rental_date_time'] = pd.to_datetime(london['start_rental_date_time'])
london['end_rental_date_time'] = pd.to_datetime(london['end_rental_date_time'])

# calculate the count of transactions on each station and each date
end = london.groupby(by = ['end_rental_date_time', 'end_station_id']).count().reset_index()
start = london.groupby(by = ['start_rental_date_time', 'start_station_id']).count().reset_index()
start = start.iloc[:,:3]
end = end.iloc[:,0:3]

# extract date and hour from datetime column
# start
start['date'] = start['start_rental_date_time'].dt.date
start['hour'] = start['start_rental_date_time'].dt.hour
start['date_hour'] = start['date'].astype(str) + ',' +  start['hour'].astype(str)
start_count = start.groupby(by = ['date_hour','start_station_id'])['rental_id'].count().reset_index()
start_count.rename(columns = {'start_station_id':'station_id', 'rental_id': 'start_count'}, inplace = True)

# end
end['date'] = end['end_rental_date_time'].dt.date
end['hour'] = end['end_rental_date_time'].dt.hour
end['date_hour'] = end['date'].astype(str) + ',' +  end['hour'].astype(str)
end_count = end.groupby(by = ['date_hour','end_station_id'])['rental_id'].count().reset_index()
end_count.rename(columns = {'end_station_id':'station_id', 'rental_id': 'end_count'}, inplace = True)
end_count['station_id'] = end_count['station_id'].astype(int)

# construct difference between start count and end count
all_count = pd.merge(start_count, end_count, how='inner', on=['station_id', 'date_hour'],  suffixes=('_left', '_right'))
all_count['diff'] =  all_count['end_count'] - all_count['start_count']

# construct station df containing geo information and places of interest

count = pd.read_csv("/content/drive/MyDrive/stat480/count.csv")
station = spark.sql("SELECT * FROM  station")
station_df = station.toPandas()

count = count.transpose()
count.columns = count.iloc[0,:]
count = count.iloc[1:,:]
count = count.rename_axis(None, axis=1)
count.index = pd.RangeIndex(start=0, stop=802, step=1)
station_df = pd.concat([station_df, count], axis = 1)
station_df[["apartment", "business","entertainment","food","government","hospital","locality","mall","park","sport","transportation"]] = station_df[["apartment", "business","entertainment","food","government","hospital","locality","mall","park","sport","transportation"]].astype(int)
station_df.to_csv("station.csv")
station_df = station_df.drop(columns = 'Unnamed: 0')

# construct ready-to-model df

model_df = pd.merge(all_count, station, how='inner', on=['station_id'],  suffixes=('_left', '_right'))
model_df[['date','hour']] = model_df['date_hour'].str.split(',', expand=True)
model_df['date'] = pd.to_datetime(model_df['date'])

# weekday dummy variables
model_df['weekday'] = model_df['date'].dt.dayofweek >=5
model_df['weekend'] = model_df['weekday'].astype(int)
# hour dummy variables
model_df['hour'] = model_df['hour'].astype(int)
dummy = pd.get_dummies(model_df.hour, prefix = 'hour')
model_df = pd.concat([model_df, dummy], axis = 1)

model_df = model_df.drop(columns = ['date_hour','longitude','latitude','weekday'])
model_df.to_csv('model_df.csv')




