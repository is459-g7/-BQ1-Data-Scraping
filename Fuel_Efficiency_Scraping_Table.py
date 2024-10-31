#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install pandas


# In[2]:


import pandas as pd

# Wikipedia page URL
url = "https://en.wikipedia.org/wiki/Fuel_economy_in_aircraft"

# Read all tables from the page
tables = pd.read_html(url)

# Select and save tables by indexing or by examining each table's content
# For this example, let's assume tables are in order of appearance
tables_to_save = {"Commuter_flights.csv": tables[1], 
                  "Regional_Flights.csv": tables[2],
                  "Short_Haul_Flights.csv": tables[3],
                  "Medium_Haul_Flights.csv": tables[4]}

for filename, table in tables_to_save.items():
    table.to_csv(filename, index=False)
    print(f"Saved {filename}")

    


# In[ ]:




