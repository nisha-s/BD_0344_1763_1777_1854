import json
import socket
import time
import sys
import random
import numpy
import threading
import ast
import csv
from threading import Lock , BoundedSemaphore

semaphore1 = BoundedSemaphore()    #for updating job status
semaphore2 = BoundedSemaphore()	   #for task queue variable 	
semaphore3 = BoundedSemaphore()    #for updating worker details
semaphore4 = BoundedSemaphore()    #for writing the logs
jobs_recieved=[]    #keeps track of all the jobs recieved
tasks = []	#list containing all the tasks to be sent to workers after scheduling
f1=open('log.csv','w',buffering=1)  #opening the log file in write mode
logs = [] 

class Worker:  #class containing details of worker
	def __init__(self,slots , port , worker_id):

		self.slots = slots
		self.max = slots
		self.port = port
		self.worker_id = worker_id

	def increase_slots(self):

		#if(self.slots < self.max):
			self.slots = self.slots + 1

	def reduce_slots(self):
		#if(self.slots >= 0)
			self.slots = self.slots - 1




incoming_job_port=5000

def fun1(port):
	host='' 
	s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	s.bind((host,port))
	while (True):
		s.listen(100)
		conn,addr=s.accept()
		while 1:
			data = conn.recv(2048)
			if data:
				logs = []
				d=data.decode()
				
				res = json.loads(d)
				writer = csv.writer(f1)      # creating a writer for log file
				now = time.strftime('%d-%m-%Y %H:%M:%S')  
				logs.append(["master",res['job_id'],now])
				l1 = []
				l1.append(res['job_id'])
				l1.append(res['map_tasks'])
				l1.append(res['reduce_tasks'])


				while(semaphore1._value == 0):
					pass
				semaphore1.acquire()
				jobs_recieved.append(l1)
				semaphore1.release()

				
				while(semaphore2._value == 0):
					pass
				semaphore2.acquire()
				for k in l1[1]:
					tasks.insert(0,k)
				semaphore2.release()
				
				while(semaphore4._value == 0):
					pass
				semaphore4.acquire()
				for i in logs:
					writer.writerow(i)
				semaphore4.release()
				
				
				
			else:
				conn.close()
				break
			
	#print(jobs_recieved[-1])
	#j = jobs_recieved[-1]
	#print(j[0] , len(j))
	#print(j)
	#print(j[1] , type(j[1][0]))
	#print(j[2] , type(j[2][0]))
	#print(j[2][0]['task_id'])
	#print(tasks , len(tasks))

					

def fun2(l , algo):
	index=0
	#print(workers[0].port , workers[0].slots)
	#for i in l:
	if(algo == "LL"):
		while(True):
			if(len(tasks) == 0):
				continue
			else:
				#dispatch a task using the LEAST LOADED scheduling algorithm	
				max_slots = 0
				max_slots_port = 0
				for i in range(0,len(workers)):
					if(workers[i].slots > max_slots):
						max_slots = workers[i].slots
						max_slots_port = workers[i].port
						index=i
				if(max_slots == 0):
					continue
				else:

					while(semaphore2._value == 0):
						pass
					semaphore2.acquire()
					message = tasks.pop()		
					semaphore2.release()


					message = str(message)	#dictionary to be sent to worker
					#print(max_slots_port)
					port=max_slots_port
					host=''
					s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					s.connect((host, port))
					s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 10)
					#message=json.dumps(i)
					s.send(message.encode())
					while(semaphore3._value == 0):
						pass
					semaphore3.acquire()
					workers[index].reduce_slots()
					semaphore3.release()
					#print(workers[index].port," : ",workers[index].slots)

	elif( algo == "RR"):
		count = 0;
		while(True):
			if(len(tasks) == 0):
				continue
			else:
				#dispatch a task using the ROUND ROBIN scheduling algorithm	
				slots = workers[count].slots
				slots_port = workers[count].port
				if(slots == 0):
					continue
				else:
	
					while(semaphore2._value == 0):
						pass
					semaphore2.acquire()
					message = tasks.pop()		
					semaphore2.release()

					message = str(message)	
					#print(slots_port)
					port=slots_port
					host=''
					s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					s.connect((host, port))
					s.send(message.encode())
				count = count+1
				if(count == len(workers)):  # only three workers 0,1,2
					count=0
	
	elif( algo == "RANDOM"):
		while(True):
			if(len(tasks) == 0):
				continue
			else:
				#dispatch a task using the RANDOM scheduling algorithm	
				num = random.randrange(0,len(workers))   # randomly generating worker id
				slots = workers[num].slots
				slots_port = workers[num].port
				if(slots == 0):
					continue
				else:
			
					while(semaphore2._value == 0):
						pass
					semaphore2.acquire()
					message = tasks.pop()		
					semaphore2.release()

					message = str(message)
					#print(slots_port)
					port=slots_port
					host=''
					s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					s.connect((host, port))
					s.send(message.encode())
	

def fun3():
	host='' 
	port=5001
	s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	s.bind((host,port))
	while (1):
		s.listen(100)
		conn,addr=s.accept()
		while 1:
			data = conn.recv(2048)
			if data:
				d=data.decode()
				res = ast.literal_eval(d) 
				#print("len of res : ",len(res))
				print(res[2]) #worker_id, free_slots, finished_task_id
				for i in range(len(workers)):
					if workers[i].worker_id==res[0]:
						while(semaphore3._value == 0):
							pass
						semaphore3.acquire()
						workers[i].slots=res[1]
						semaphore3.release()
				if(len(res[2]) != 0):    # checking if any task completion is done by the worker
					for task_completed in res[2]:
						if(task_completed[2] == 'M'):   #check if completed task is whether mapper or reducer
							for m in jobs_recieved:
								if(m[0] == task_completed[0]):  #search for the corresponding job
									all_map_tasks = m[1]
									for k in all_map_tasks:
										if(k['task_id'] == task_completed):  #update in the list that this map task is completed
											while(semaphore1._value == 0):
												pass
											semaphore1.acquire()
											k['duration'] = 0
											semaphore1.release()
											break

	
									all_mappers_done = True										
									for k in all_map_tasks:    # check if all map tasks are finished for this job
										if(k['duration'] != 0):
											all_mappers_done = False
											break
										
									if(all_mappers_done == True):   #if all map tasks of a job is finished , push reduce tasks to the task queue
											print("all map tasks is finished for job " , m[0])
											reduce_tasks = m[2]
											while(semaphore2._value == 0):
												pass
											semaphore2.acquire()
											for q in reduce_tasks:
												tasks.append(q)  #pushing reduce tasks of the corresponding job to task queue
											semaphore2.release()
											
						if(task_completed[2] == 'R'):    #if the completed task is reducer 
							for m in jobs_recieved:
								if(m[0] == task_completed[0]):  #search for the corresponding job
									all_reduce_tasks = m[2]
									for k in all_reduce_tasks:
										if(k['task_id'] == task_completed):
											while(semaphore1._value == 0):
												pass
											semaphore1.acquire()
											k['duration'] = 0  # update in the list that this reduce task is done
											semaphore1.release()
											break;
									all_reducers_done = True   
									for k in all_reduce_tasks:    # check if all mappers are done
										if(k['duration'] != 0):
											all_reducers_done = False
											break
									if(all_reducers_done == True):
										print("The execution of job is finsihed with id : " , m[0])      #job is finished
			else:
				conn.close()
				break



algo=""
conf=""
if len(sys.argv) == 3:
	conf = sys.argv[1]
	algo = sys.argv[2]
else:
	for i in range(1,len(sys.argv)-1):
		conf = conf + " " + sys.argv[i]
	algo = sys.argv[len(sys.argv)-1]
		
conf = conf.strip()
print("algo = ",algo)
print("conf = ",conf)

#f = open('Copy of config.json','r') 
f = open(conf,'r') 
data = json.load(f) 
l=[]
for i in data['workers']: 
    l.append(i)
f.close()

number_of_workers = len(l)
workers = []       #workers[i] is a object of class 'workers'
for i in l:
	w = Worker(i['slots'],i['port'] , i['worker_id'])
	workers.append(w)





t1=threading.Thread(target=fun1,args=(incoming_job_port,))
t3=threading.Thread(target=fun2,args=(l,algo,))
t4=threading.Thread(target=fun3)

t1.start()
t3.start()
t4.start()




t1.join()
t3.join()
t4.join()



















