import glob
import json
import numpy
import os
import re
import textwrap
import time
from Queue import Queue
from datetime import datetime
from enum import Enum
from softdev import epics, models, log
from threading import Thread
from twisted.internet import reactor

from . import isara, msgs

PUCK_LIST = [
    '1A', '2A', '3A', '4A', '5A',
    '1B', '2B', '3B', '4B', '5B', '6B',
    '1C', '2C', '3C', '4C', '5C',
    '1D', '2D', '3D', '4D', '5D', '6D',
    '1E', '2E', '3E', '4E', '5E',
    '1F', '2F',
]

NUM_PUCKS = 29
NUM_PUCK_SAMPLES = 16
NUM_PLATES = 8
NUM_WELLS = 192
NUM_ROW_WELLS = 24
STATUS_TIME = 0.025

STATUS_PATT = re.compile(r'^(?P<context>\w+)\((?P<msg>.*?)\)?$')

logger = log.get_module_logger(__name__)


class ToolType(Enum):
    NONE, UNIPUCK, ROTATING, PLATE, LASER, DOUBLE = range(6)


class PuckType(Enum):
    ACTOR, UNIPUCK = range(2)


StatusType = msgs.StatusType


class OffOn(Enum):
    OFF, ON = range(2)


class GoodBad(Enum):
    BAD, GOOD = range(2)


class OpenClosed(Enum):
    CLOSED, OPEN = range(2)


class CryoLevel(Enum):
    UNKNOWN, TOO_LOW, NORMAL, LOW, HIGH, TOO_HIGH = range(6)


class Position(Enum):
    HOME, SOAK, GONIO, DEWAR, UNKNOWN = range(5)


class ModeType(Enum):
    MANUAL, AUTO = range(2)


class ActiveType(Enum):
    INACTIVE, ACTIVE = range(2)


class EnableType(Enum):
    DISABLED, ENABLED = range(2)


class ErrorType(Enum):
    OK, WAITING, WARNING, ERROR = range(4)


class AuntISARA(models.Model):
    connected = models.Enum('CONNECTED', choices=ActiveType, default=0, desc="Robot Connection")
    enabled = models.Enum('ENABLED', choices=EnableType, default=1, desc="Robot Control")
    status = models.Enum('STATUS', choices=StatusType, desc="Robot Status")
    health = models.Enum('HEALTH', choices=ErrorType, desc="Robot Health")
    log = models.String('LOG', desc="Sample Operation Message", max_length=1024)
    warning = models.String('WARNING', max_length=1024, desc='Warning message')
    help = models.String('HELP', max_length=1024, desc='Help')

    # Inputs and Outputs
    input0_fbk = models.BinaryInput('STATE:INP0', desc='Digital Inputs 00-15')
    input1_fbk = models.BinaryInput('STATE:INP1', desc='Digital Inputs 16-31')
    input2_fbk = models.BinaryInput('STATE:INP2', desc='Digital Inputs 32-47')
    input3_fbk = models.BinaryInput('STATE:INP3', desc='Digital Inputs 48-63')

    emergency_ok = models.Enum('INP:emerg', choices=GoodBad, desc='Emergency/Air OK')
    collision_ok = models.Enum('INP:colSensor', choices=GoodBad, desc='Collision Sensor OK')
    cryo_level = models.Enum('INP:cryoLevel', choices=CryoLevel, desc='Cryo Level')
    gonio_ready = models.Enum('INP:gonioRdy', choices=OffOn, desc='Gonio Ready')
    sample_detected = models.Enum('INP:smplOnGonio', choices=OffOn, desc='Sample on Gonio')
    cryojet_fbk = models.Enum('INP:cryojet', choices=OffOn, desc='Cryojet Back')
    heartbeat = models.Enum('INP:heartbeat', choices=OffOn, desc='Heart beat')

    output0_fbk = models.BinaryInput('STATE:OUT0', desc='Digital Outpus 00-15')
    output1_fbk = models.BinaryInput('STATE:OUT1', desc='Digital Oututs 16-31')
    output2_fbk = models.BinaryInput('STATE:OUT2', desc='Digital Oututs 32-47')
    output3_fbk = models.BinaryInput('STATE:OUT3', desc='Digital Oututs 48-63')


    # Status
    mode_fbk = models.Enum('STATE:mode', choices=ModeType, desc='Control Mode')
    error_fbk = models.Integer('STATE:error', desc='Error Code')
    position_fbk = models.String('STATE:pos', max_length=40, desc='Position')
    default_fbk = models.Enum('STATE:default', choices=OffOn, desc='Default Status')
    tool_fbk = models.Enum('STATE:tool', choices=ToolType, desc='Tool Status')
    tool_open_fbk = models.Enum('STATE:toolOpen', choices=OpenClosed, desc='Tool Open')
    path_fbk = models.String('STATE:path', max_length=40, desc='Path Name')
    puck_tool_fbk = models.Integer('STATE:toolPuck', min_val=0, max_val=29, desc='Puck On tool')
    puck_tool2_fbk = models.Integer('STATE:tool2Puck', min_val=0, max_val=29, desc='Puck On tool2')
    puck_diff_fbk = models.Integer('STATE:diffPuck', min_val=0, max_val=29, desc='Puck On Diff')
    sample_tool_fbk = models.Integer('STATE:toolSmpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On tool Sample')
    sample_tool2_fbk = models.Integer('STATE:tool2Smpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On tool2 Sample')
    sample_diff_fbk = models.Integer('STATE:diffSmpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On Diff Sample')
    plate_fbk = models.Integer('STATE:plate', min_val=0, max_val=NUM_PLATES, desc='Plate Status')
    barcode_fbk = models.String('STATE:barcode', max_length=40, desc='Barcode Status')
    power_fbk = models.Enum('STATE:power', choices=OffOn, desc='Robot Power')
    running_fbk = models.Enum('STATE:running', choices=OffOn, desc='Path Running')
    trajectory_fbk = models.Enum('STATE:traj', choices=OffOn, desc='Traj Running')
    autofill_fbk = models.Enum('STATE:autofill', choices=OffOn, desc='Auto-Fill')
    approach_fbk = models.Enum('STATE:approach', choices=OffOn, desc='Approaching')
    magnet_fbk = models.Enum('STATE:magnet', choices=OffOn, desc='Magnet')
    heater_fbk = models.Enum('STATE:heater', choices=OffOn, desc='Heater')
    lid_fbk = models.Enum('STATE:lidOpen', choices=OpenClosed, desc='Lid')
    software_fbk = models.Enum('STATE:software', choices=OffOn, desc='Software')
    remote_speed_fbk = models.Enum('STATE:remSpeed', choices=OffOn, desc='Remote Speed')

    speed_fbk = models.Integer('STATE:speed', min_val=0, max_val=100, units='%', desc='Speed Ratio')
    pos_dew_fbk = models.String('STATE:posDewar', max_length=40, desc='Position in Dewar')
    soak_count_fbk = models.Integer('STATE:soakCount', desc='Soak Count')
    pucks_fbk = models.String('STATE:pucks', max_length=40, desc='Puck Detection')
    pucks_bit0 = models.BinaryInput('STATE:pucks:bit0', desc='Puck Detection')
    pucks_bit1 = models.BinaryInput('STATE:pucks:bit1', desc='Puck Detection')
    xpos_fbk = models.Float('STATE:posX', prec=2, desc='X-Pos')
    ypos_fbk = models.Float('STATE:posY', prec=2, desc='Y-Pos')
    zpos_fbk = models.Float('STATE:posZ', prec=2, desc='Z-Pos')
    rxpos_fbk = models.Float('STATE:posRX', prec=2, desc='RX-Pos')
    rypos_fbk = models.Float('STATE:posRY', prec=2, desc='RY-Pos')
    rzpos_fbk = models.Float('STATE:posRZ', prec=2, desc='RZ-Pos')
    mounted_fbk = models.String('STATE:onDiff', max_length=40, desc='On Gonio')
    tooled_fbk = models.String('STATE:onTool', max_length=40, desc='On Tool')
    tooled2_fbk = models.String('STATE:onTool2', max_length=40, desc='On Tool 2')

    # Params
    next_param = models.String('PAR:nextPort', max_length=40, default='', desc='Port')
    barcode_param = models.Enum('PAR:barcode', choices=OffOn, default=0, desc='Barcode')
    tool_param = models.Enum('PAR:tool', choices=ToolType, default=1, desc='Tool')
    puck_param = models.Integer('PAR:puck', min_val=0, max_val=NUM_PUCKS, default=0, desc='Puck')
    sample_param = models.Integer('PAR:smpl', min_val=0, max_val=NUM_PUCK_SAMPLES, default=0, desc='Sample')
    plate_param = models.Integer('PAR:plate', min_val=0, max_val=NUM_PLATES, desc='Plate')
    type_param = models.Enum('PAR:puckType', choices=PuckType, default=PuckType.UNIPUCK.value, desc='Puck Type')
    pos_name = models.String('PAR:posName', max_length=40, default='', desc='Position Name')
    pos_force = models.Enum('PAR:posForce', choices=OffOn, default=0, desc='Overwrite Position')
    pos_tolerance = models.Float('PAR:posTol', default=0.1, prec=2, desc='Position Tolerance')

    # New Autofill Levels
    low_ln2_level = models.Integer('PAR:lowLN2', min_val=0, max_val=100, default=0, desc='Low LN2 Level')
    high_ln2_level = models.Integer('PAR:highLN2', min_val=0, max_val=100, default=0, desc='High LN2 Level')

    # General Commands
    power_cmd = models.Toggle('CMD:power', desc='Power')
    panic_cmd = models.Toggle('CMD:panic', desc='Panic')
    abort_cmd = models.Toggle('CMD:abort', desc='Abort')
    pause_cmd = models.Toggle('CMD:pause', desc='Pause')
    reset_cmd = models.Toggle('CMD:reset', desc='Reset')
    restart_cmd = models.Toggle('CMD:restart', desc='Restart')
    clear_barcode_cmd = models.Toggle('CMD:clrBarcode', desc='Clear Barcode')
    lid_cmd = models.Enum('CMD:lid', choices=('Close Lid', 'Open Lid'), desc='Lid')
    tool_cmd = models.Enum('CMD:tool', choices=('Close Tool', 'Open Tool'), desc='Tool')
    faster_cmd = models.Toggle('CMD:faster', desc='Speed Up')
    slower_cmd = models.Toggle('CMD:slower', desc='Speed Down')
    magnet_enable = models.Toggle('CMD:magnet', desc='Magnet')
    heater_enable = models.Toggle('CMD:heater', desc='Heater')
    speed_enable = models.Toggle('CMD:speed', desc='Remote Speed')
    approach_enable = models.Toggle('CMD:approach', desc='Approaching')
    running_enable = models.Toggle('CMD:running', desc='Path Running')
    autofill_enable = models.Toggle('CMD:autofill', desc='LN2 AutoFill')

    # Trajectory Commands
    home_cmd = models.Toggle('CMD:home', desc='Home')
    safe_cmd = models.Toggle('CMD:safe', desc='Safe')
    put_cmd = models.Toggle('CMD:put', desc='Put')
    get_cmd = models.Toggle('CMD:get', desc='Get')
    getput_cmd = models.Toggle('CMD:getPut', desc='Get Put')
    barcode_cmd = models.Toggle('CMD:barcode', desc='Barcode')
    back_cmd = models.Toggle('CMD:back', desc='Back')
    soak_cmd = models.Toggle('CMD:soak', desc='Soak')
    dry_cmd = models.Toggle('CMD:dry', desc='Dry')
    pick_cmd = models.Toggle('CMD:pick', desc='Pick')
    change_tool_cmd = models.Toggle('CMD:chgTool', desc='Change Tool')

    calib_cmd = models.Toggle('CMD:toolCal', desc='Tool Calib')
    teach_gonio_cmd = models.Toggle('CMD:teachGonio', desc='Teach Gonio')
    teach_puck_cmd = models.Toggle('CMD:teachPuck', desc='Teach Puck')

    # Maintenance commands
    clear_cmd = models.Toggle('CMD:clear', desc='Clear')
    set_diff_cmd = models.Toggle('CMD:setDiffSmpl', desc='Set Sample')
    set_tool_cmd = models.Toggle('CMD:setToolSmpl', desc='Set Tool')
    set_tool2_cmd = models.Toggle('CMD:setTool2Smpl', desc='Set Tool2')
    reset_params = models.Toggle('CMD:resetParams', desc='Reset Parameters')
    reset_motion = models.Toggle('CMD:resetMotion', desc='Reset Motion')
    save_pos_cmd = models.Toggle('CMD:savePos', desc='Save Position')

    # Simplified commands
    dismount_cmd = models.Toggle('CMD:dismount', desc='Dismount')
    mount_cmd = models.Toggle('CMD:mount', desc='Mount')


def port2args(port):
    # converts '1A16' to puck=1, sample=16, tool=2 for UNIPUCK where NUM_PUCK_SAMPLES = 16
    # converts 'P2' to plate=2, tool=3 for Plates
    if len(port) < 3: return {}
    args = {}
    if port.startswith('P'):
        args = {
            'tool': ToolType.PLATE.value,
            'plate': zero_int(port[1]),
            'mode': 'plate'
        }
    else:
        args = {
            'tool': ToolType.UNIPUCK.value,
            'puck': PUCK_LIST.index(port[:2]) + 1,
            'sample': zero_int(port[2:]),
            'mode': 'puck'
        }
    return args


def pin2port(puck, pin):
    # converts puck=1, pin=16 to '1A16'
    if puck > 0 and pin > 0:
        return '{}{}'.format(PUCK_LIST[puck - 1], pin)
    else:
        return ''


def plate2port(plate):
    # converts plate=1, to 'P1'
    if plate:
        return 'P{}'.format(plate, )
    else:
        return ''


def zero_int(text):
    try:
        return int(text)
    except ValueError:
        return 0


def minus_int(text):
    try:
        return int(text)
    except ValueError:
        return -1


def name_to_tool(text):
    return {
        'simple': ToolType.UNIPUCK.value,
        'laser': ToolType.LASER.value,
        'flange': ToolType.NONE.value,
    }.get(text.lower(), 1)


class PositionManager(object):
    """Manages robot positions including saving/loading and testing for current position"""

    def __init__(self, name, directory):
        """
        :param name: Name of positions file
        :param directory: directory where file is located
        """
        self.name = name
        self.directory = directory
        self.raw = {}
        self.info = {}
        self.load()

    def is_ready(self):
        return bool(self.info)

    def load(self):
        """Load most recent positions from directory"""
        self.raw = {}
        data_files = glob.glob(os.path.join(self.directory, '{}*.dat'.format(self.name)))
        if data_files:
            data_files.sort(key=lambda x: -os.path.getmtime(x))
            latest = data_files[0]

            logger.info('Using Position File: {}'.format(latest))
            with open(latest, 'r') as fobj:
                self.raw = json.load(fobj)
        self.setup()

    def setup(self):
        """Prepare arrays for calculating positions"""
        if self.raw:
            self.info = {
                'names': numpy.array(list(self.raw.keys())),
                'coords': numpy.array([(v['x'], v['y'], v['z']) for v in self.raw.values()]),
                'tolerances': numpy.array([v['tol'] for v in self.raw.values()])
            }
        else:
            self.info = {}

    def save(self):
        """
        Save current positions to a file. If previous file was older than today, create a new one with todays's date
        as a suffix.
        """
        logger.debug('Saving positions ...')
        positions_file = '{}-{}.dat'.format(self.name, datetime.today().strftime('%Y%m%d'))
        with open(os.path.join(self.directory, positions_file), 'w') as fobj:
            json.dump(self.raw, fobj, indent=2)

    def add(self, name, coords, tolerance, replace=False):
        if replace or name not in self.info['names']:
            self.raw[name] = {
                'x': coords[0],
                'y': coords[1],
                'z': coords[2],
                'rx': coords[3],
                'ry': coords[4],
                'rz': coords[5],
                'tol': tolerance,
            }
            self.setup()
            self.save()

    def check(self, coords):
        """
        Determine named position from given coordinates
        :param coords: (x, y, z, rx, ry, rz)
        :return: named position or 'UNKNOWN'
        """
        if self.info:
            pos = numpy.array(coords[:3])
            distances = (numpy.power((self.info['coords'] - pos), 2)).sum(axis=1)
            tolerances = numpy.power(self.info['tolerances'], 2)
            within = ((tolerances - distances) >= 0)
            if any(within):
                return self.info['names'][within][0]
            else:
                return 'UNKNOWN'
        else:
            return 'UNKNOWN'


class AuntISARAApp(object):
    def __init__(self, device_name, address, command_port=1000, status_port=10000, positions='positions'):
        self.app_directory = os.getcwd()
        self.ioc = AuntISARA(device_name, callbacks=self)

        self.responses = Queue()
        self.statuses = Queue()
        self.commands = Queue()

        self.command_on = False
        self.status_on = False
        self.user_enabled = False
        self.ready = False

        self.standby_active = False
        self.dewar_pucks = set()

        # Prepare connection/protocols
        self.command_client = isara.CommandFactory(self)
        self.status_client = isara.StatusFactory(self)
        self.pending_clients = {self.command_client.protocol.message_type, self.status_client.protocol.message_type}
        reactor.connectTCP(address, status_port, self.status_client)
        reactor.connectTCP(address, command_port, self.command_client)

        # status pvs and conversion types
        # maps status position to record, converter pairs
        self.status_map = {
            0: (self.ioc.power_fbk, int),
            1: (self.ioc.mode_fbk, int),
            2: (self.ioc.default_fbk, int),
            3: (self.ioc.tool_fbk, name_to_tool),
            4: (self.ioc.path_fbk, str),
            5: (self.ioc.puck_tool_fbk, minus_int),
            6: (self.ioc.sample_tool_fbk, minus_int),
            7: (self.ioc.puck_diff_fbk, minus_int),
            8: (self.ioc.sample_diff_fbk, minus_int),
            9: (self.ioc.plate_fbk, minus_int),
            11: (self.ioc.barcode_fbk, str),
            12: (self.ioc.running_fbk, int),
            13: (self.ioc.autofill_fbk, int),
            15: (self.ioc.speed_fbk, int),
            18: (self.ioc.pos_dew_fbk, zero_int),
            20: (self.ioc.puck_tool2_fbk, minus_int),
            21: (self.ioc.sample_tool2_fbk, minus_int),
            22: (self.ioc.soak_count_fbk, int),
        }
        self.position_map = [
            self.ioc.xpos_fbk, self.ioc.ypos_fbk, self.ioc.zpos_fbk,
            self.ioc.rxpos_fbk, self.ioc.rypos_fbk, self.ioc.rzpos_fbk
        ]
        self.input_map = {
            1: self.ioc.emergency_ok,
            2: self.ioc.collision_ok,
            8: self.ioc.gonio_ready,
            47: self.ioc.cryojet_fbk,
            31: self.ioc.heartbeat,
        }
        self.rev_input_map = {
            9: self.ioc.sample_detected,
        }
        self.output_map = {
            1: self.ioc.tool_open_fbk,
            4: self.ioc.approach_fbk,
            5: self.ioc.trajectory_fbk,
            6: self.ioc.magnet_fbk,
            8: self.ioc.software_fbk,
            12: self.ioc.heater_fbk
        }

        self.mounting = False
        self.aborted = False

        self.positions = PositionManager(positions, self.app_directory)

    def sender(self):
        """
        Main method which sends commands to the robot from the commands queue. New commands are placed in the queue
        to be sent through this method. This method is called within the sender thread.
        """
        self.command_on = True
        epics.threads_init()
        while self.command_on:
            command, full_cmd = self.commands.get()
            logger.debug('< {}'.format(full_cmd))
            try:
                self.command_client.send_message(full_cmd)
            except Exception as e:
                logger.error('{}: {}'.format(full_cmd, e))
            time.sleep(0.01)

    def status_monitor(self):
        """
        Sends status commands and receives status replies from ISARA.
        """
        epics.threads_init()
        self.status_on = True
        cmd_index = 0
        commands = ['state', 'di', 'di2', 'do', 'state', 'position', 'message']
        while self.status_on:
            command = commands[cmd_index]
            self.status_client.send_message(command)
            success, reply = self.wait_for_status(command, timeout=3)
            if success:
                self.parse_status(command, reply)
            else:
                logger.warning(message)
            cmd_index = (cmd_index + 1) % len(commands)
            time.sleep(STATUS_TIME)

    def response_monitor(self):
        """
        Receives responses from commands sent to ISARA.
        """
        self.command_on = True
        epics.threads_init()
        while self.command_on:
            message = self.responses.get()
            self.ioc.log.put(message)
            time.sleep(STATUS_TIME)

    def disconnect(self, client_type):
        """
        Called when a protocol client disconnects
        :param client_type:  The client_type of the client which disconnected.
        """
        self.pending_clients.add(client_type)
        self.status_on = False
        self.command_on = False
        self.ioc.connected.put(0)

    def connect(self, client_type):
        """
        Called when a protocol client connects
        :param client_type: The client_type of the connected client.
        :return:
        """
        self.pending_clients.remove(client_type)

        # all clients connected
        if not self.pending_clients:
            self.responses.queue.clear()
            self.commands.queue.clear()
            self.statuses.queue.clear()

            command_thread = Thread(target=self.sender)
            status_thread = Thread(target=self.status_monitor)
            response_thread = Thread(target=self.response_monitor)

            command_thread.setDaemon(True)
            status_thread.setDaemon(True)
            response_thread.setDaemon(True)

            command_thread.start()
            status_thread.start()
            response_thread.start()

            self.ready = True
            self.ioc.connected.put(1)
            logger.warn('Controller ready!')
        else:
            self.ready = False

    def shutdown(self):
        """
        Shutdown service
        """
        logger.warn('Shutting down ...')
        self.status_on = False
        self.command_on = False
        self.ready = False
        self.ioc.shutdown()

    @staticmethod
    def wait_for_queue(context, timeout=5, queue=None):
        context, reply = queue.get()
        while timeout > 0 and not context.startswith(context):
            timeout -= 0.01
            time.sleep(0.01)
            context, reply = queue.get()

        if timeout > 0:
            return True, reply
        else:
            logger.warn('Timeout waiting for response "{}"'.format(context))
            return False, 'Timeout waiting for response to command "{}"'.format(context)

    def wait_for_response(self, command, timeout=5):
        return self.wait_for_queue(command, timeout=timeout, queue=self.responses)

    def wait_for_status(self, command, timeout=5):
        return self.wait_for_queue(command, timeout=timeout, queue=self.statuses)

    def wait_for_position(self, *positions):
        timeout = 30
        while timeout > 0 and self.ioc.position_fbk.get() not in positions:
            timeout -= 0.01
            time.sleep(0.01)
        if timeout > 0:
            return True
        else:
            logger.warn('Timeout waiting for positions "{}"'.format(states))
            return False

    def wait_for_state(self, *states):
        timeout = 30
        state_values = [s.value for s in states]
        while timeout > 0 and self.ioc.status.get() not in state_values:
            timeout -= 0.01
            time.sleep(0.01)
        if timeout > 0:
            return True
        else:
            logger.warn('Timeout waiting for states "{}"'.format(states))
            return False

    def wait_in_state(self, state):
        timeout = 30
        while timeout > 0 and self.ioc.status.get() == state.value:
            timeout -= 0.01
            time.sleep(0.01)
        if timeout > 0:
            return True
        else:
            logger.warn('Timeout in state "{}"'.format(state))
            return False

    def ready_for_commands(self):
        return self.ready and self.ioc.enabled.get() and self.ioc.connected.get()

    @staticmethod
    def make_args(tool=0, puck=0, sample=0, puck_type=PuckType.UNIPUCK.value, x_off=0, y_off=0, z_off=0, **kwargs):
        return (tool, puck, sample) + (0,) * 4 + (puck_type, 0, 0) + (x_off, y_off, z_off)

    def send_command(self, command, *args):
        self.standby_active = False
        if self.ready_for_commands():
            if args:
                cmd = '{}({})'.format(command, ','.join([str(arg) for arg in args]))
            else:
                cmd = command
            self.commands.put((command, cmd))

    def receive_message(self, message, message_type):
        logger.debug('{}: <- {}'.format(message_type, message))
        if message_type == isara.MessageType.RESPONSE:
            self.responses.put(message)
        elif message_type == isara.MessageType.STATUS:
            m = STATUS_PATT.match(message)
            if m:
                info = m.groupdict()
                self.statuses.put((info['context'], info['msg']))
            else:
                self.statuses.put(('message', message))
        else:
            self.statuses.put(('message', message))

    def parse_inputs(self, bitstring):
        inputs = [self.ioc.input0_fbk, self.ioc.input1_fbk, self.ioc.input2_fbk, self.ioc.input3_fbk]

        for pv, bits in zip(inputs, textwrap.wrap(bitstring, 16)):
            pv.put(int(bits, 2))
        for i, bit in enumerate(bitstring):
            if i in self.input_map:
                self.input_map[i].put(int(bit))
            if i in self.rev_input_map:
                self.rev_input_map[i].put((int(bit) + 1) % 2)

        # setup LN2 status & alarms
        hihi, hi, lo, lolo = map(int, bitstring[3:7])
        if not hihi:
            self.ioc.cryo_level.put(CryoLevel.TOO_HIGH.value)
        elif not lolo:
            self.ioc.cryo_level.put(CryoLevel.TOO_LOW.value)
        elif hi:
            self.ioc.cryo_level.put(CryoLevel.HIGH.value)
        elif lo:
            self.ioc.cryo_level.put(CryoLevel.LOW.value)
        else:
            self.ioc.cryo_level.put(CryoLevel.NORMAL.value)

    def parse_outputs(self, bitstring):
        outputs = [self.ioc.output0_fbk, self.ioc.output1_fbk, self.ioc.output2_fbk, self.ioc.output3_fbk]

        for pv, bits in zip(outputs, textwrap.wrap(bitstring, 16)):
            pv.put(int(bits, 2))
        for i, bit in enumerate(bitstring):
            if i in self.output_map:
                pv = self.output_map[i]
                pv.put(int(bit))

    def calc_position(self):
        coords = numpy.array(
            [
                self.ioc.xpos_fbk.get(), self.ioc.ypos_fbk.get(), self.ioc.zpos_fbk.get(),
                self.ioc.rxpos_fbk.get(), self.ioc.rypos_fbk.get(), self.ioc.rzpos_fbk.get(),
            ]
        )

        name = self.positions.check(coords)
        self.ioc.position_fbk.put(name)

    def require_position(self, *allowed):
        if not self.positions.is_ready():
            self.warn('No positions have been defined')
            self.ioc.help.put(
                'Please move the robot manually and save positions named `{}`'.format(' | '.join(allowed)))
            return False

        current = self.ioc.position_fbk.get()
        for pos in allowed:
            if re.match('^' + pos + '(?:_\w*)?$', current):
                return True
        self.warn('Command allowed only from ` {} ` position'.format(' | '.join(allowed)))
        self.ioc.help.put('Please move the robot into the correct position and the re-issue the command')

    def require_tool(self, *tools):
        if self.ioc.tool_fbk.get() in [t.value for t in tools]:
            return True
        else:
            self.warn('Invalid tool for command!')

    def warn(self, msg):
        self.ioc.warning.put('{} {}'.format(datetime.now().strftime('%b/%d %H:%M:%S'), msg))

    def parse_status(self, context, message):
        # state
        fault_active = False
        wait_active = False
        if context == 'state':
            strings = message.split(',')
            values = []
            for i, txt in enumerate(strings):
                if not i in self.status_map: continue
                record, converter = self.status_map[i]
                try:
                    value = converter(txt)
                    values.append(value)
                    record.put(value)
                except ValueError:
                    logger.warning('Unable to parse state: {}'.format(txt))

            # set mounted state
            puck, pin = values[7], values[8]
            port = pin2port(puck, pin)
            if port != self.ioc.mounted_fbk.get():
                self.ioc.mounted_fbk.put(port)

            # set tooled state
            puck, pin = values[5], values[6]
            port = pin2port(puck, pin)
            if port != self.ioc.tooled_fbk.get():
                self.ioc.tooled_fbk.put(port)

            # Not available on our ISARA
            # set tooled2 state
            # puck, pin = values[20], values[21]
            # port = pin2port(puck, pin)
            # if port != self.ioc.tooled2_fbk.get():
            #     self.ioc.tooled2_fbk.put(port)

        # Positions
        if context == 'position':
            for i, value in enumerate(message.split(',')):
                try:
                    self.position_map[i].put(float(value))
                except ValueError:
                    logger.warning('Unable to parse position: {}'.format(message))
            self.calc_position()

        # puck detection
        if context == 'di2':
            bitstring = message.replace(',', '')
            self.ioc.pucks_fbk.put(bitstring)
            bit0, bit1 = textwrap.wrap(bitstring, 16)
            self.ioc.pucks_bit0.put(int(bit0[::-1], 2))
            self.ioc.pucks_bit1.put(int(bit1[::-1], 2))

        # process inputs
        if context == 'di':
            bitstring = message.replace(',', '').ljust(64, '0')
            self.parse_inputs(bitstring)

        # process outputs
        if context == 'do':
            bitstring = message.replace(',', '').ljust(64, '0')
            self.parse_outputs(bitstring)

        if context == 'message':
            if message:
                warning, help, state, bit = msgs.parse_error(message)
                bitarray = list(bin(self.ioc.error_fbk.get())[2:].rjust(32, '0'))

                if bit is not None:
                    bitarray[bit] = '1'
                    new_value = int(''.join(bitarray), 2)
                    if new_value != self.ioc.error_fbk.get():
                        self.ioc.error_fbk.put(new_value)
                    if state == StatusType.FAULT:
                        self.ioc.health.put(ErrorType.ERROR.value)
                        fault_active = True
                    elif state == StatusType.WAITING:
                        wait_active = True
                else:
                    self.ioc.error_fbk.put(0)
            else:
                self.ioc.health.put(ErrorType.OK.value)
                self.ioc.error_fbk.put(0)

        # determine robot state
        next_status = None
        cur_status = self.ioc.status.get()
        if fault_active:
            next_status = StatusType.FAULT.value
        elif wait_active:
            next_status = StatusType.WAITING.value
        elif self.ioc.running_fbk.get() and self.standby_active:
            next_status = StatusType.STANDBY.value
        elif self.ioc.running_fbk.get() and self.ioc.trajectory_fbk.get():
            next_status = StatusType.BUSY.value
        elif not self.ioc.running_fbk.get():
            next_status = StatusType.IDLE.value

        if next_status is not None and next_status != cur_status:
            self.ioc.status.put(next_status)

    # def mount_operation(self, cmd, args):
    #     epics.threads_init()
    #     allowed_tools = (ToolType.UNIPUCK, ToolType.ROTATING, ToolType.DOUBLE, ToolType.PLATE)
    #     puck_tools = (ToolType.UNIPUCK.value, ToolType.ROTATING.value, ToolType.DOUBLE.value)
    #     if not self.mounting:
    #         self.mounting = True
    #         self.aborted = False
    #
    #         port = self.ioc.next_param.get().strip()
    #         current = self.ioc.mounted_fbk.get().strip()
    #         params = port2args(port)
    #
    #         if not (params and all(params.values())):
    #             self.warn('Invalid Port for mounting: {}'.format(port))
    #             self.mounting = False
    #             return
    #
    #         if params.get('tool') in puck_tools:
    #             if self.ioc.barcode_param.get():
    #                 command = 'put_bcrd' if not current else 'getput_bcrd'
    #             else:
    #                 command = 'put' if not current else 'getput'
    #         else:
    #             command = 'putplate' if not current else 'getputplate'
    #
    #         current_tool = self.ioc.tool_fbk.get()
    #         if self.require_position('SOAK') and self.require_tool(*allowed_tools):
    #             if params['tool'] == current_tool and current_tool in puck_tools:
    #                 args = self.make_args(
    #                     tool=params['tool'], puck=params['puck'], sample=params['sample'],
    #                     puck_type=params['tool']
    #                 )
    #             else:
    #                 args = (
    #                     ToolType.PLATE.value, 0, 0, 0, 0, ioc.plate_param.get()
    #                 )
    #             self.send_command(command, *args)
    #
    #         self.mounting = False

    # callbacks
    def do_mount_cmd(self, pv, value, ioc):
        if value and self.require_position('SOAK') and not self.mounting:
            self.standby_active = False
            self.mounting = True
            port = ioc.next_param.get().strip()
            current = ioc.mounted_fbk.get().strip()
            command = 'put' if not current else 'getput'
            params = port2args(port)
            if params and all(params.values()):
                if params['mode'] == 'puck':
                    ioc.tool_param.put(params['tool'])
                    ioc.puck_param.put(params['puck'])
                    ioc.sample_param.put(params['sample'])
                elif params['mode'] == 'plate':
                    ioc.tool_param.put(params['tool'])
                    ioc.plate_param.put(params['plate'])
                else:
                    self.warn('Invalid Port parameters for mounting: {}'.format(params))
                    self.mounting = False
                    return

                if command == 'put':
                    self.ioc.put_cmd.put(1)
                elif command == 'getput':
                    self.ioc.getput_cmd.put(1)
            else:
                self.warn('Invalid Port for mounting: {}'.format(port))
                self.mounting = False

    def do_dismount_cmd(self, pv, value, ioc):
        if value and self.require_position('SOAK') and not self.mounting:
            self.mounting = True
            self.standby_active = False
            current = ioc.mounted_fbk.get().strip()
            params = port2args(current)
            if params and all(params.values()):
                ioc.tool_param.put(params['tool'])
                self.ioc.get_cmd.put(1)
            else:
                self.warn('Invalid port or sample not mounted')
                self.mounting = False

    def do_power_cmd(self, pv, value, ioc):
        if value:
            cmd = 'off' if ioc.power_fbk.get() else 'on'
            self.send_command(cmd)

    def do_panic_cmd(self, pv, value, ioc):
        if value:
            self.send_command('panic')

    def do_abort_cmd(self, pv, value, ioc):
        if value:
            self.send_command('abort')
            self.aborted = True
            self.mounting = False

    def do_pause_cmd(self, pv, value, ioc):
        if value:
            self.send_command('pause')

    def do_reset_cmd(self, pv, value, ioc):
        if value:
            self.send_command('reset')
            self.ioc.error_fbk.put(0)
            self.ioc.help.put('')
            self.ioc.warning.put('')
            self.mounting = False

    def do_restart_cmd(self, pv, value, ioc):
        if value:
            self.send_command('restart')

    def do_clear_barcode_cmd(self, pv, value, ioc):
        if value:
            self.send_command('clearbcrd')

    def do_lid_cmd(self, pv, value, ioc):
        if value:
            self.send_command('openlid')
        else:
            self.send_command('closelid')

    def do_tool_cmd(self, pv, value, ioc):
        if value:
            self.send_command('opentool')
        else:
            self.send_command('closetool')

    def do_faster_cmd(self, pv, value, ioc):
        if value:
            self.send_command('speedup')

    def do_slower_cmd(self, pv, value, ioc):
        if value:
            self.send_command('speeddown')

    def do_magnet_enable(self, pv, value, ioc):
        if value:
            cmd = 'magnetoff' if ioc.magnet_fbk.get() else 'magneton'
            self.send_command(cmd)

    def do_heater_enable(self, pv, value, ioc):
        if value:
            cmd = 'heateroff' if ioc.heater_fbk.get() else 'heateron'
            self.send_command(cmd)

    def do_speed_enable(self, pv, value, ioc):
        if value:
            st, cmd = (0, 'remotespeedoff') if ioc.remote_speed_fbk.get() else (1, 'remotespeedon')
            self.send_command(cmd)
            ioc.remote_speed_fbk.put(st)

    def do_approach_enable(self, pv, value, ioc):
        if value:
            cmd = 'cryoOFF' if ioc.approach_fbk.get() else 'cryoON'
            self.send_command(cmd, ioc.tool_fbk.get())

    def do_running_enable(self, pv, value, ioc):
        if value:
            cmd = 'trajOFF' if ioc.running_fbk.get() else 'trajON'
            self.send_command(cmd, ioc.tool_fbk.get())

    def do_autofill_enable(self, pv, value, ioc):
        if value:
            cmd = 'reguloff' if ioc.autofill_fbk.get() else 'regulon'
            self.send_command(cmd)

    def do_home_cmd(self, pv, value, ioc):
        if value and self.require_position('SOAK', 'HOME', 'UNKNOWN'):
            self.send_command('home', ioc.tool_fbk.get())

    def do_change_tool_cmd(self, pv, value, ioc):
        if value and self.require_position('HOME'):
            if ioc.tool_param.get() != ioc.tool_fbk.get():
                self.send_command('home', ioc.tool_param.get())
            else:
                self.warn('Requested tool already present, command ignored')

    def do_safe_cmd(self, pv, value, ioc):
        if value:
            self.send_command('safe', ioc.tool_fbk.get())

    def do_put_cmd(self, pv, value, ioc):
        allowed_tools = (ToolType.UNIPUCK, ToolType.ROTATING, ToolType.DOUBLE, ToolType.PLATE)
        if value and self.require_position('SOAK') and self.require_tool(*allowed_tools):
            if self.ioc.tool_fbk.get() in [ToolType.UNIPUCK.value, ToolType.ROTATING.value, ToolType.DOUBLE.value]:
                args = self.make_args(
                    tool=ioc.tool_param.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                    puck_type=ioc.type_param.get()
                )
                cmd = 'put_bcrd' if ioc.barcode_param.get() else 'put'
            else:
                args = (
                    ToolType.PLATE.value, 0, 0, 0, 0, ioc.plate_param.get()
                )
                cmd = 'putplate'
            self.send_command(cmd, *args)

    def do_get_cmd(self, pv, value, ioc):
        allowed_tools = (ToolType.UNIPUCK, ToolType.ROTATING, ToolType.DOUBLE, ToolType.PLATE)
        if value and self.require_position('SOAK') and self.require_tool(*allowed_tools):
            if self.ioc.tool_fbk.get() in [ToolType.UNIPUCK.value, ToolType.ROTATING.value, ToolType.DOUBLE.value]:
                cmd = 'get'
            else:
                cmd = 'getplate'
            self.send_command(cmd, ioc.tool_param.get())

    def do_getput_cmd(self, pv, value, ioc):
        allowed_tools = (ToolType.UNIPUCK, ToolType.ROTATING, ToolType.DOUBLE, ToolType.PLATE)
        if value and self.require_position('SOAK') and self.require_tool(*allowed_tools):

            if self.ioc.tool_fbk.get() in [ToolType.UNIPUCK.value, ToolType.ROTATING.value, ToolType.DOUBLE.value]:
                cmd = 'getput_bcrd' if ioc.barcode_param.get() else 'getput'
                args = self.make_args(
                    tool=ioc.tool_param.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                    puck_type=ioc.type_param.get()
                )
            else:
                cmd = 'getplate'
                args = (
                    ToolType.PLATE.value, 0, 0, 0, 0, ioc.plate_param.get()
                )
            self.send_command(cmd, *args)

    def do_barcode_cmd(self, pv, value, ioc):
        if value and self.require_position('SOAK'):
            args = self.make_args(
                tool=ioc.tool_fbk.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            self.send_command('barcode', *args)

    def do_back_cmd(self, pv, value, ioc):
        if value and self.require_position('HOME'):
            if ioc.tooled_fbk.get():
                self.send_command('back', ioc.tool_fbk.get())
            else:
                self.warn('No sample on tool, command ignored')

    def do_soak_cmd(self, pv, value, ioc):
        allowed = (ToolType.DOUBLE, ToolType.UNIPUCK, ToolType.ROTATING)
        if value and self.require_position('HOME') and self.require_tool(*allowed):
            self.send_command('soak', ioc.tool_fbk.get())

    def do_dry_cmd(self, pv, value, ioc):
        allowed = (ToolType.DOUBLE, ToolType.UNIPUCK, ToolType.ROTATING)
        if value and self.require_position('SOAK', 'HOME') and self.require_tool(*allowed):
            self.send_command('dry', ioc.tool_fbk.get())

    def do_pick_cmd(self, pv, value, ioc):
        if value and self.require_tool(ToolType.DOUBLE):
            args = self.make_args(
                tool=ToolType.DOUBLE.value, puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            self.send_command('pick', *args)

    def do_calib_cmd(self, pv, value, ioc):
        if value and self.require_position('HOME'):
            self.send_command('toolcal', ioc.tool_fbk.get())

    def do_teach_gonio_cmd(self, pv, value, ioc):
        if value and self.require_position('HOME') and self.require_tool(ToolType.LASER):
            self.send_command('teach_gonio', ToolType.LASER.value)

    def do_teach_puck_cmd(self, pv, value, ioc):
        if value and self.require_tool(ToolType.LASER) and self.require_position('HOME'):
            if ioc.puck_param.get():
                self.send_command('teach_puck', ToolType.LASER.value, ioc.puck_param.get())
            else:
                self.warn('Please select a puck number')

    def do_set_diff_cmd(self, pv, value, ioc):
        if value:
            current = ioc.next_param.get()
            if current.strip():
                params = port2args(current)
                self.send_command('setdiffr', params['puck'], params['sample'], ioc.type_param.get())
            else:
                self.warn('No Sample specified')

    def do_set_tool_cmd(self, pv, value, ioc):
        if value:
            current = ioc.next_param.get()
            if current.strip():
                params = port2args(current)
                self.send_command('settool', params['puck'], params['sample'], ioc.type_param.get())
            else:
                self.warn('No Sample specified')

    def do_set_tool2_cmd(self, pv, value, ioc):
        if value and self.require_tool(ToolType.DOUBLE):
            current = ioc.next_param.get()
            if current.strip():
                params = port2args(current)
                self.send_command('settool2', params['puck'], params['sample'], ioc.type_param.get())
            else:
                self.warn('No Sample specified')

    def do_clear_cmd(self, pv, value, ioc):
        if value:
            self.send_command('clear memory')

    def do_reset_params(self, pv, value, ioc):
        if value:
            self.send_command('reset parameters')

    def do_reset_motion(self, pv, value, ioc):
        if value:
            self.send_command('resetMotion')

    def do_pucks_fbk(self, pv, value, ioc):
        if len(value) != NUM_PUCKS:
            logger.error('Puck Detection does not contain {} values!'.format(NUM_PUCKS))
        else:
            pucks_detected = {v[1] for v in zip(value, PUCK_LIST) if v[0] == '1'}
            added = pucks_detected - self.dewar_pucks
            removed = self.dewar_pucks - pucks_detected
            self.dewar_pucks = pucks_detected
            logger.info('Pucks changed: added={}, removed={}'.format(list(added), list(removed)))

            # If currently mounting a puck and it is removed abort
            on_tool = ioc.tooled_fbk.get().strip()
            if on_tool and on_tool[:2] in removed and ioc.status.get() in [StatusType.BUSY.value]:
                msg = 'Target puck removed while mounting. Aborting! Manual recovery required.'
                logger.error(msg)
                self.warn(msg)
                self.send_command('abort')

    def do_save_pos_cmd(self, pv, value, ioc):
        if value and ioc.pos_name.get().strip():
            pos_name = ioc.pos_name.get().strip().replace(' ', '_')
            tolerance = ioc.pos_tolerance.get()
            replace = bool(ioc.pos_force.get())
            coords = numpy.array(
                [
                    ioc.xpos_fbk.get(), ioc.ypos_fbk.get(), ioc.zpos_fbk.get(),
                    ioc.rxpos_fbk.get(), ioc.rypos_fbk.get(), ioc.rzpos_fbk.get(),
                ]
            )
            self.positions.add(pos_name, coords, tolerance, replace)
            ioc.pos_force.put(0)
            ioc.pos_name.put('')

    def do_status(self, pv, value, ioc):
        if value == 0:
            if ioc.error_fbk.get():
                ioc.reset_cmd.put(1)
            self.mounting = False

    def do_position_fbk(self, pv, value, ioc):
        if 'DRY' in value:
            self.standby_active = True

    def do_error_fbk(self, pv, value, ioc):
        if value:
            bitarray = list(bin(value)[2:].rjust(32, '0'))
            errors = filter(None, [msgs.MESSAGES.get(i) for i, bit in enumerate(bitarray) if bit == '1'])
            if errors:
                texts = [(err.get('description', ''), err.get('help')) for err in errors]
                warnings, help = zip(*texts)
                warning_text = '; '.join(warnings)
                help_text = '; '.join(help)
                if warning_text:
                    self.warn(warning_text)
                if help_text:
                    ioc.help.put(help)
        else:
            ioc.help.put('')

    def do_low_ln2_level(self, pv, value, ioc):
        if value:
            self.send_command('setLowLN2', value)

    def do_high_ln2_level(self, pv, value, ioc):
        if value:
            self.send_command('setHighLN2', value)


