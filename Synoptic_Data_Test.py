#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
import synoptic

df = synoptic.TimeSeries(
    stid="ukbkb",
    recent=timedelta(hours=96),
    vars="air_temp,dew_point_temperature",
    units="english",
).df()

plt.figure(figsize=(10, 5))
ax = sns.lineplot(
    df,
    x="date_time",
    y="value",
    hue="variable",
    #marker="_",
    palette=["tab:red", "tab:green"],
)
ax.set_title(f"{df['stid'][0]}: {df['name'][0]}")
ax.set_ylabel("Temperature (°F)")
ax.set_xlabel("")
ax.grid(alpha=0.5, zorder=0, lw=0.5, ls="--")
ax.legend(title="")


# In[7]:


dir(sns)


# In[ ]:




