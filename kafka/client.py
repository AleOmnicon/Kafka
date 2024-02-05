from Producer import Producer
import threading as t

BROKER = '192.168.1.50:9092'
q = list()

def add_to_q(elem,id):
    q.append([elem,id])

prod1 = Producer(BROKER,'prod_py_1','topic1')

x=0
while x<10:
    prod1.produce(x)
    x=x+1


