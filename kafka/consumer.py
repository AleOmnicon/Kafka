from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka.errors import IllegalStateError

BROKER = '192.168.1.50:9092'

cons = KafkaConsumer(group_id='win_py_consumers',bootstrap_servers=BROKER, client_id='consumer1_win_py')

partitions = cons.partitions_for_topic('topic1')
tps = []
for p in partitions:
    tps.append(TopicPartition('topic1',p))

cons.assign(tps)

print(cons.bootstrap_connected())

# cons.beginning_offsets(tps)
cons.seek_to_beginning()
# data = cons.poll(100)
# print(data)
# data = cons.poll(110,update_offsets=False)
# print(data.keys())
# print(data.values())
for m in cons:
    print("%s:%d:%d: key=%s value=%s" % (m.topic, m.partition, m.offset, m.key, m.value))


def pull_data_from_beginning(topic, partitions = None, keys=None):
    '''
        Retrives all data saved in the broker
        from a given topic and specific partitions (if defined) with specific keys (if defined)

        Parameters
        ----------
        topic : the topic from which the data will be retrieved
        partitions : if specified gets data only from given partitions. If not gets data from
            all partitions
        keys : if specified gets data that matches one of the given keys values. If not
            gets all data without considering its key

        Returns
        -------
        list : a list containing selected data
    '''
    data = []
    p = partitions
    tps = []
    c = KafkaConsumer(group_id='query_consumer', bootstrap_servers=BROKER,client_id='win_py_puller')

    if c.bootstrap_connected() == False:
        return False

    if p == None:
        p = c.partitions_for_topic(topic)

    for pr in p:
        tps.append(TopicPartition(topic,pr))
    try:
        c.assign(tps)
    except IllegalStateError as e:
        return e

    c.seek_to_beginning()

    

    return data
