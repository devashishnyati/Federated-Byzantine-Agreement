from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys

port_number = int(sys.argv[1])

class MulticastPingClient(DatagramProtocol):

    def startProtocol(self):
        self.transport.joinGroup('228.0.0.5')
        self.transport.write(bytes('foo:$10 request', 'utf-8'), ('228.0.0.5', port_number))
        print('foo:$10 sent to node:', port_number)
        self.transport.write(bytes('foo:$20 request', 'utf-8'), ('228.0.0.5', port_number))
        print('foo:$20 sent to node:', port_number)
        self.transport.write(bytes('foo:$30 request', 'utf-8'), ('228.0.0.5', port_number))
        print('foo:$30 sent to node:',port_number)
        self.transport.write(bytes('bar:$10 request', 'utf-8'), ('228.0.0.5', port_number))
        print('bar:$10 sent to node:',port_number)
        self.transport.write(bytes('bar:$20 request', 'utf-8'), ('228.0.0.5', port_number))
        print('bar:$20 sent to node:',port_number)
        self.transport.write(bytes('bar:$30 request', 'utf-8'), ('228.0.0.5', port_number))
        print('bar:$30 sent to node: {}\n'.format(port_number))

    def datagramReceived(self, datagram, address):
        print('Transaction {} received from {}'.format(repr(datagram.decode('UTF-8')), repr(address)))


reactor.listenMulticast(8888, MulticastPingClient(), listenMultiple=True)
reactor.run()
#reactor.stop()