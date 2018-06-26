# -*- coding: utf-8 -*-
"""
Created on Tue Jun 26 14:48:53 2018

@author: man37cha
"""

import mysql.connector
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import glob


all_files = glob.glob('C:\\Users\\man37cha\\Desktop\\EBUS\\Data\\EBUS TEST DATA\\*') # * means all if need specific format then *.csv
#print(all_files)  For DEBUGGING
for  fn in range(len(all_files)):
            

            file = open(all_files[fn], 'r')
            file_content = file.readlines()   # This gives a list of values line by line from the text file , Notice we used file.readlines() fucntion here.
            list_size=len(file_content)  # size of the above formed list stored.
            
            data_full=[]
            #Now Accessing Every Element from the Python List.
            for element in range(list_size):
                 data=file_content[element].split('#') 
                 data_full.append(data)  
            np_data_full=np.array(data_full)    
            
            pd_np_data_full=pd.DataFrame(np_data_full)
            
            #print(pd_np_data_full)       For DEBUGGING
            
            df = pd.DataFrame(columns=['date','time','SOC','MILAGE','SPEED']) #creating an empty dataframe
            
            for i in range(list_size):
                if df.empty:     #Checking if the dataframe is empty.
                   df = df.append({'date': pd_np_data_full.iloc[i][0], 'time':pd_np_data_full.iloc[i][1]},ignore_index=True) #Appendong DATE and Time in the empty dataframe
                   if pd_np_data_full.iloc[i][2]=='SOC':                #checking if the third element of thedatframe created from the given data is SOC , MILAGE < OR SPEED  and accordingly pusing it under the column of the new dataframe
                           df.iat[-1,2]=pd_np_data_full.iloc[i][3]
                   elif pd_np_data_full.iloc[i][2]=='MILAGE':
                           df.iat[-1,3]=pd_np_data_full.iloc[i][3]
                   elif pd_np_data_full.iloc[i][2]=='SPEED':
                           df.iat[-1,4]=pd_np_data_full.iloc[i][3]
                else:
                    if df.iloc[-1][0] == pd_np_data_full.iloc[i][0] and df.iloc[-1][1]!=pd_np_data_full.iloc[i][1]:
                       df = df.append({'date': pd_np_data_full.iloc[i][0], 'time':pd_np_data_full.iloc[i][1]},ignore_index=True)
                       if pd_np_data_full.iloc[i][2]=='SOC':
                           df.iat[-1,2]=pd_np_data_full.iloc[i][3]
                       elif pd_np_data_full.iloc[i][2]=='MILAGE':
                           df.iat[-1,3]=pd_np_data_full.iloc[i][3]
                       elif pd_np_data_full.iloc[i][2]=='SPEED':
                           df.iat[-1,4]=pd_np_data_full.iloc[i][3]
                    else:
                      if pd_np_data_full.iloc[i][2]=='SOC':
                           df.iat[-1,2]=pd_np_data_full.iloc[i][3]
                      elif pd_np_data_full.iloc[i][2]=='MILAGE':
                           df.iat[-1,3]=pd_np_data_full.iloc[i][3]
                      elif pd_np_data_full.iloc[i][2]=='SPEED':
                           df.iat[-1,4]=pd_np_data_full.iloc[i][3]
              
            df['MILAGE'] = df['MILAGE'].astype(str).str.replace('\D+', '')   #removing all characters othere then numericals
            df['SPEED'] = df['SPEED'].astype(str).str.replace('\D+', '')   #removing all characters othere then numericals
            df['SOC'] = df['SOC'].astype(str).str.replace('\D+', '')   #removing all characters othere then numericals
            df["DATE"] = df["date"].map(str) + str(" ")+ df["time"]    #combining the column date and time
            df['DATE'] = pd.to_datetime(df['DATE'])                    #coverting the date+time string into DATE datatype
            df = df.drop('date',1)    #Dropping the old date column
            df = df.drop('time',1)          #droping the old time column
            print(fn)
            print(df)    #  For DEBUGGING
       #     engine = create_engine('mysql+mysqlconnector://root:1234567@[localhost]/ebus', echo=False)
       #     df.to_sql(name='ebus_panda',con=engine,if_exists='append',index= False)
            
