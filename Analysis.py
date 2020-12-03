import numpy as np
import pandas as pd
import time
import datetime
import matplotlib.pyplot as plt
import sys
import json


med = []
me = []

algo = ["task","job"]

# Round Robin

df = pd.read_csv("log1.csv", header=None)
df1 = pd.read_csv("log.csv", header=None)
#print(df.head(12))

df.to_csv("logf.csv",header=["worker","task","start"], index = False)
df = pd.read_csv("logf.csv")

df1.to_csv("log1f.csv",header=["master","job","time"], index = False)
df1 = pd.read_csv("log1f.csv")


df_sort = df.sort_values(["task"])
df_sort = df_sort.reset_index(drop=True)
df_sort["end"] = 0
#print(df_sort.head(10))

df = pd.DataFrame({'worker':df_sort['worker'].iloc[::2].values,'task':df_sort['task'].iloc[::2].values,'start':df_sort['start'].iloc[::2].values, 'end':df_sort['start'].iloc[1::2].values})
#print(df.head())

df['time'] = abs(pd.to_datetime(df['end']) - pd.to_datetime(df['start']))
#print(df.head())

mean_task = df['time'].mean()
mean_task = mean_task.total_seconds()
#print(type(mean_task))
print("mean_task = ", mean_task)

median_task = df['time'].median()
median_task = median_task.total_seconds()
print("median_task_rr = ", median_task)

me.append(mean_task)
med.append(median_task)

new = df["task"].str.split("_", n = 1, expand = True)
df['job'] = new[0]
df['map/red'] = new[1]
#print(df.head())


#map1 = df.groupby('job').head(1)
red1 = df.groupby('job').tail(1)
red1 = red1.reset_index(drop=True)


job_diff = abs(pd.to_datetime(red1['end']) - pd.to_datetime(df1['time']))
#print(job_diff)

mean_job = job_diff.mean()
mean_job = mean_job.total_seconds()
median_job = job_diff.median()
median_job = median_job.total_seconds()
print("mean_job = ", mean_job)
print("median_job_ = ", median_job)

me.append(mean_job)
med.append(median_job)


# Plot for task
indx = np.arange(len(algo))
bar_width = 0.35

fig, ax = plt.subplots()
barMean=ax.bar(indx-bar_width/2, me, bar_width, label='mean') 
barMedian=ax.bar(indx+bar_width/2, med, bar_width, label='median') 
 
ax.set_xticks(indx)
ax.set_xticklabels(algo)

plt.title("Task vs Job")

#ax.set_yticks()
#ax.set_yticklabels()

ax.legend()

#plt.show()
plt.show()

df = pd.read_csv("log2.csv", header=None)


conf=""
if len(sys.argv) == 2:
	conf = sys.argv[1]
else:
	for i in range(1,len(sys.argv)-1):
		conf = conf + " " + sys.argv[i]	
conf = conf.strip()

f = open(conf,"r")
data = json.load(f)
l = []
for i in data['workers']:
	l.append(i)
f.close()

n = 3

df.to_csv("log3.csv",header=["worker_id","tasks_running","timestamp"], index = False)
df = pd.read_csv("log3.csv")

#print(type(df['timestamp'][0]))

least_time = datetime.datetime.strptime(df['timestamp'][0],'%d-%m-%Y %H:%M:%S')
#print(least_time)
for i in df.index:
    df['timestamp'][i] = abs(datetime.datetime.strptime(df['timestamp'][i],'%d-%m-%Y %H:%M:%S') - least_time)

df = df.groupby(df.worker_id)
leng = len(l)
lst =[ ]
for i in range(0,4):
	try:
		dff = df.get_group(i+1)
	except:
		continue
	lst.append(dff)

#print(lst)

for i in range(0,leng):
	for j in lst[i].index:
	    lst[i]['timestamp'][j] = lst[i]['timestamp'][j].total_seconds()
	plt.plot(lst[i]['timestamp'] , lst[i]['tasks_running'])
	tit = "worker"+(str)(i+1)
	plt.title(tit)
	plt.xlabel("time")
	plt.ylabel("No. of tasks scheduled")
	plt.show()
	
f = open("log1.csv", "w")
f.truncate(0)
f.close()

f = open("log.csv", "w")
f.truncate(0)
f.close()

f = open("log2.csv", "w")
f.truncate(0)
f.close()

f = open("log3.csv", "w")
f.truncate(0)
f.close()

f = open("logf.csv", "w")
f.truncate(0)
f.close()

