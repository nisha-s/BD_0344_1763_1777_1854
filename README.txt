Procedure to run the code :

At first open 5 terminals:
1 terminal for Master.py
1 terminal for Requests.py
3 terminals for Worker.py (as there are 3 workers in this conf file)

Execution of Scheduling Algorithm implementation in Master:
Round Robin     :     python3 Master.py config.json RR 
Least Loaded    :     python3 Master.py config.json LL
Random          :     python3 Master.py config.json RANDOM

Execution of Worker:
syntax : python3 Worker.py port_number worker_id
Example : python3 Worker.py 4000 1

Execution of Requests:
Syntax : python3 Requests.py number_of_jobs
Example : python3 Requests.py 10



OPEN ALL THE 5 TERMINALS
Complete execution of project (TO BE STRICTLY FOLLOWED IN ORDER):

terminal 1 : python3 Master.py config.json RR 
terminal 2 : python3 Worker.py 4000 1
terminal 3 : python3 Worker.py 4001 2
terminal 4 : python3 Worker.py 4002 3
terminal 5 : python3 Requests.py 10

After execution log.csv file is generated

terminal 6 : python3 Analysis.py

Run Analysis.py to look at the Summary of the YACS
