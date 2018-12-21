from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import pickledb
import sys

port_number = int(sys.argv[1])
quorum = [3000,3001,3002,3003]
msg_received = False
db = pickledb.load('assignment3_{}.db'.format(port_number), False)

class MulticastPingPong(DatagramProtocol):
    msg_dictionary = {}
    commits= []
    added =[]
    getall_list =[]

    def startProtocol(self):
        """
        Called after protocol has started listening.
        """
        self.transport.setTTL(5)
        self.transport.joinGroup('228.0.0.5')
        self.transport.joinGroup('228.0.0.6')

    def datagramReceived(self, datagram, address):
        global msg_received
        
        msg = datagram.decode('UTF-8')
        transaction,status = msg.split(" ")
        self.msg_dictionary.setdefault(transaction,0) 
        self.msg_dictionary.setdefault(msg,0)
        key, value = transaction.split(':$')

        if status == 'request':
            preprepare_message = '{} preprepare'.format(transaction)
            prepare_message = '{} voting'.format(transaction)
            quorum.remove(port_number)
            for port in quorum:
                self.transport.write(bytes(preprepare_message, 'utf-8'), ('228.0.0.6', port))
                self.transport.write(bytes(prepare_message, 'utf-8'), ('228.0.0.6', port))
            print('Protocol Message: {} received from {}'.format(msg, repr(address)))
            quorum.append(port_number)
   
        elif status == 'preprepare':
            prepare_message = '{} voting'.format(transaction)
            quorum.remove(port_number)
            for port in quorum:
                self.transport.write(bytes(prepare_message, 'utf-8'), ('228.0.0.6', port))
            print('Protocol Message: {} received from {}'.format(msg, repr(address)))
            quorum.append(port_number)

        elif status == 'voting':
            #Voting
            self.msg_dictionary[transaction]+=1
            if self.msg_dictionary[transaction] >=len(quorum)/2 and transaction not in self.commits:
                print('Protocol Message: {} received from {}'.format(msg, repr(address)))
                commit_message = '{} commit'.format(transaction)
                self.commits.append(transaction)
                quorum.remove(port_number)
                for port in quorum:
                    self.transport.write(bytes(commit_message, 'utf-8'), ('228.0.0.6', port))
                quorum.append(port_number)
        
        elif status == 'commit':
            self.msg_dictionary[msg] += 1
            if self.msg_dictionary[msg] >=len(quorum)/2 and transaction not in self.added:
                print('\nProtocol Message: {} received'.format(msg))
                self.added.append(transaction)
                if db.get(key):
                    money_sum = int(''.join(db.get(key)))
                    money_sum +=int(value)
                    db.set(key,str(money_sum))
                else:
                    db.set(key,value)
                self.getall_list = db.getall()
                for item in self.getall_list:
                    print('Database Update: {}:${}'.format(item,db.get(item)))
                db.dump()
                final_value = key +':$' + db.get(key)
                self.transport.write(bytes(final_value, 'utf-8'), ('228.0.0.5', 8888))

reactor.listenMulticast(port_number, MulticastPingPong(),listenMultiple=True)
reactor.run()