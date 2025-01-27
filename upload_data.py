#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


pd.__version__


# In[3]:


df = pd.read_csv('green_tripdata_2019-10.csv', nrows = 100)


# In[4]:


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# In[5]:


from sqlalchemy import create_engine
connection_string = 'postgresql://root:root@localhost:5432/ny_taxi'
engine = create_engine(connection_string)


# In[6]:


print(pd.io.sql.get_schema(df, name='green_taxi_data', con=engine))


# In[7]:


df_iter = pd.read_csv('green_tripdata_2019-10.csv', iterator=True, chunksize=10000)


# In[8]:


df = next(df_iter)


# In[9]:


len(df)


# In[10]:


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


# In[11]:


df


# In[ ]:


df.head(n=0).to_sql(name='green_taxi_data',con=engine, if_exists='replace')


# In[ ]:


get_ipython().run_line_magic('time', "df.to_sql(name='green_taxi_data',con=engine, if_exists='append')")


# In[ ]:


from time import time


# In[ ]:


while True:
    t_start = time()
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name='green_taxi_data',con=engine, if_exists='append')
    t_end = time()
    print('inserted another chunk..., took %.3f second' % (t_end - t_start))

