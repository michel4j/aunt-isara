
from enum import Enum
from devioc import log
from twisted.internet import reactor, protocol
from twisted.protocols.basic import LineReceiver

logger = log.get_module_logger(__name__)


class MessageType(Enum):
    RESPONSE, STATUS = range(2)


class CommandProtocol(LineReceiver):
    delimiter = b'\r'
    protocol_name = 'Command Link'
    message_type = MessageType.RESPONSE

    def __init__(self, factory):
        self.factory = factory

    def connectionMade(self):
        reactor.addSystemEventTrigger('before', 'shutdown', self.transport.abortConnection)
        logger.warn('{} Connected!'.format(self.protocol_name))

    def connectionLost(self, reason=protocol.connectionDone):
        logger.warning('{} Disconnected: {}'.format(self.protocol_name, reason.getErrorMessage()))

    def lineReceived(self, line):
        self.receive_message(line.decode('utf-8').strip())

    def send_message(self, message):
        if self.transport:
            self.sendLine(message.encode('utf-8'))

    def receive_message(self, message):
        self.factory.receive_message(message, self.message_type)


class StatusProtocol(CommandProtocol):
    protocol_name = 'Status Link'
    message_type = MessageType.STATUS


class CommandFactory(protocol.ReconnectingClientFactory):
    protocol = CommandProtocol

    def __init__(self, application):
        self.application = application
        self.ready = False
        self.client = None

    def buildProtocol(self, address):
        logger.log(log.IMPORTANT, '{} Ready: {}'.format(address, self.protocol.protocol_name))
        self.client = self.protocol(self)
        self.resetDelay()
        self.ready = True
        self.application.connect(self.protocol.message_type)
        return self.client

    def clientConnectionLost(self, connector, reason):
        self.disconnect()
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        self.disconnect()
        protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def send_message(self, message):
        if self.ready and self.client:
            self.client.send_message(message)
        else:
            logger.error('Client not connected. Command ignored!')

    def receive_message(self, message, message_type):
        self.application.receive_message(message, message_type)

    def disconnect(self):
        self.ready = False
        self.application.disconnect(self.protocol.message_type)


class StatusFactory(CommandFactory):
    protocol = StatusProtocol

