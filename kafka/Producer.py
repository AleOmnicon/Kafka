from kafka import KafkaProducer

class Producer():
    '''
        A class for a simple KafkaProducer.
        it will be connected to a broker and publish to a single topic
        
    '''
    
    def __init__(self,bootstrap_servers, name, topic) -> None:
        '''Initialize Producer class using KafkaPorducer

            Parameters
            ----------
                bootstrap_servers : 'host[:port]' string (or list of 'host[:port]' strings) 
                    that the producer should contact to bootstrap initial cluster metadata.
                    This does not have to be the full node list.
                    It just needs to have at least one broker that will respond to a Metadata API Request.\n
                name : the name you want to assign to this Producer instance\n
                topic: this is a simple producer, therefore it will send data only to this single specified topic\n
        '''
        self.topic = topic.__str__()
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,client_id=name,acks='all',retries=5,linger_ms=5)
        if self.producer.bootstrap_connected():
            print('producer',name,'connected to',bootstrap_servers)
        else:
            print('producer',name,'failed to connect to',bootstrap_servers)

    def produce(self, mex, key=None):
        '''Produce message to connected broker

        Parameters
        ----------
            mex : message to publish to initialized broker and topic\n
            key : this value determines which partition the message will go to.
                messages with the same key will always go in the same partition.
                if not defined messages wil be distributed roud robin into available partitions.
        '''
        
        ok_mex = mex.__str__().encode()
        if key != None:
            ok_key = key.__str__().encode()
            self.producer.send(self.topic, ok_mex, ok_key)
        else:
            self.producer.send(self.topic, ok_mex)

        self.producer.flush()

    





# name = 'py_prod_1'
# topic = 'topic1'
# MACCHINA_ENG = '192.168.1.50:9092'
# producer = KafkaProducer(bootstrap_servers=MACCHINA_ENG,client_id=name,acks='all',retries=50,linger_ms=5)
# q = list([12,"3",123,123,5,65,7,3,3,2,7,65,2345,3,'Marcello'])
# key = 'win_py_producer1'.encode()

# print('producer', name, 'ready to publish')
# while True:
#     while q.__len__():
#         producer.send(topic, f"{q.pop()}".encode(), key)
#         producer.flush()
#     print('waiting more data')
#     time.sleep(1)
        


# topic1_partitions = producer.partitions_for('topic1')
# future1 = producer.send('topic1', b'messaggio da win_py1', key)
# future2 = producer.send('topic1', b'messaggio da win_py2', key)

# try:
#     record_metadata2 = future2.get(timeout=10)
#     record_metadata1 = future1.get(timeout=10)
# except KafkaError:
#     print('storto')
    
# print(record_metadata1.topic,record_metadata2.topic)
# print(record_metadata1.partition,record_metadata2.partition)
# print(record_metadata1.offset,record_metadata2.offset)



# x=1
# for _ in range(10):
#     print(producer.send(topic='topic1', value=b'MESSAGGIO DA WINDOWS PY %d'%x,key=b'windows_py'))
#     x += 1
#     time.sleep(0.05)

