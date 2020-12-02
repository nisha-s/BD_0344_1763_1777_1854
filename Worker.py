import json
import socket
import time
import sys
import random
import numpy
import threading
import ast
import csv
#import pandas as pd
from threading import Lock , BoundedSemaphore

#f1=open('Log_data.csv','w',newline='')


#dur=[]

class Worker:

	def __init__(self, slots, port, worker_id):
		self.slots=slots
		self.max=slots
		self.port=port
		self.worker_id=worker_id
		self.dur=[]
		self.task_id=[]
		self.log=[]
		
	def increase_slots(self):
		lock.acquire()
		self.slots=self.slots+1
		lock.release()
		return self.slots
		
	def decrease_slots(self):
		lock.acquire()
		self.slots=self.slots-1
		lock.release()
		return self.slots

	def fun1(self, port):
		
		host='' 
		s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		s.bind((host,port))
		while (1):
			s.listen(100)
			conn,addr=s.accept()
			while 1:
				data = conn.recv(2048)
				if data:
					d=data.decode()
					
					#print(d,type(d))
					res=ast.literal_eval(d)
					
					writer = csv.writer(f1)
					now = time.strftime('%d-%m-%Y %H:%M:%S')
					self.log.append([worker_id,res['task_id'],now])
					#k=res['task_id']
					#writer.writerow([k,now])
					#res = json.loads(d) 
					#print("Duration of task : ",res['duration'])
					while(semaphore1._value == 0):
						pass
					semaphore1.acquire()
					
					self.dur.append(res['duration'])
					self.task_id.append(res['task_id'])					
					
					semaphore1.release()
					
					
					#now1 = time.strftime('%d-%m-%Y %H:%M:%S')
					#writer.writerow([res['task_id'],now1])
					
					
					
					print(self.log)
					print(self.dur)
					
					self.slots=self.decrease_slots()
					print("Number of free slots : ",self.slots)
					
					'''while(semaphore2._value == 0):
						pass
					semaphore2.acquire()'''	
					for i in self.log:
						writer.writerow(i)
					#semaphore2.release()
						
					self.log=[]
					
					
					
					
				else:
					conn.close()
					break

	def fun2(self):
		
		
		while True:
			while(semaphore1._value == 0):
				pass
			semaphore1.acquire()
			self.dur = [x - 1 for x in self.dur]
			semaphore1.release()
			if len(self.dur) != 0:
				
				print(self.dur)
			
			"""
			for i in range(0,len(self.dur)):
				if self.dur[i]==0:
					self.dur.pop(i)
					print("Number of free slots : ",self.increase_slots())
			"""
			finished_task_id=[]
			indices=[i for i,x in enumerate(self.dur) if x==0] 
			for i in indices:
				finished_task_id.append(self.task_id[i])
			
			writer = csv.writer(f1)
			now_end = time.strftime('%d-%m-%Y %H:%M:%S')
			
			'''while(semaphore2._value == 0):
				pass
			semaphore2.acquire()'''
			for k in finished_task_id:
				writer.writerow([worker_id,k,now_end])
			#semaphore2.release()
			
			self.task_id = [x for x in self.task_id if x not in finished_task_id]
			while 0 in self.dur:
				self.dur.remove(0)
				self.slots=self.increase_slots()
				print("Number of free slots : ",self.slots)
			time.sleep(1)
			writer1 = csv.writer(f2)
			writer1.writerow([worker_id,len(self.dur) , now_end])
			#print("slots : ",slots)
			port=5001
			host=''
			s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((host, port))
			lis=[self.worker_id,self.slots,finished_task_id]
			message=json.dumps(lis)
			s.send(message.encode())
			
		


	def run(self):
		t1=threading.Thread(target=self.fun1, args=(port,))
		t2=threading.Thread(target=self.fun2)
		t1.start()
		t2.start()
		t1.join()
		t2.join()




semaphore1 = BoundedSemaphore()
lock = threading.Lock()
semaphore2 = BoundedSemaphore()


port=(int)(sys.argv[1])
worker_id=(int)(sys.argv[2])
print(port,":",worker_id)
x=[]
f = open('config.json','r') 
data = json.load(f) 
l=[]
for i in data['workers']: 
   		l.append(i)
f.close() 
slots=0
for i in l:
#print(i)
	if i['worker_id']==worker_id:
		slots=i['slots']
		break

#filename='f'+(str)(worker_id)+'.csv'
f1=open('log1.csv','a',buffering=1)
f2 = open('log2.csv','a',buffering=1)
obj=Worker(slots,port,worker_id)
obj.run()


















