import glob
import json
import os
import re
import textwrap
import time

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum, IntFlag, IntEnum, auto
from functools import wraps
from itertools import cycle
from queue import Queue
from threading import Thread
from typing import Any

from parsefire.parser import parse_text

import gepics
import numpy
from devioc import models, log
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
STATUS_TIME = 0.05
DEWAR_PIN_Z = -410

STATUS_PATT = re.compile(r'^(?P<context>\w+)\((?P<msg>.*?)\)?$')
PORT_PATT = re.compile(r'^(?P<puck>[1-6][A-F])(?P<pin>[1-9][0-6]?)$')

logger = log.get_module_logger(__name__)

executor = ThreadPoolExecutor(max_workers=5)

STATE_SPECS = {
    'fields': [
        "<int:power>,<int:auto>,<int:default>,<str:tool>,<str:path>,<int:a_puck>,<int:a_pin>,"
        "<int:gonio_puck>,<int:gonio_pin>,<int:plate>,<str:dummy1>,<str:barcode>,<int:running>,"
        "<int:autofill>,<str:dummy2>,<int:speed>,<float:gonio_x>,<float:gonio_y>,<float:gonio_z>,"
        "<str:dummy3>,<int:b_puck>,<int:b_pin>,<int:soak_count>,<int:a_open>,<int:b_open>,"
        "<float:low_temp>,<float:high_temp>"
    ]
}

def async_operation(f):
    """
    Run the specified function asynchronously in a thread. Return values will not be available
    :param f: function or method
    """

    def new_f(*args, **kwargs):
        gepics.threads_init()
        return f(*args, **kwargs)

    @wraps(f)
    def _f(*args, **kwargs):
        return executor.submit(new_f, *args, **kwargs)

    return _f


class ToolType(IntEnum):
    NONE, UNIPUCK, ROTATING, PLATE, LASER, DOUBLE = range(6)


class PuckType(IntEnum):
    ACTOR, UNIPUCK = range(2)


StatusType = msgs.StatusType


class OffOn(IntEnum):
    OFF, ON = range(2)


class GoodBad(IntEnum):
    BAD, GOOD = range(2)


class OpenClosed(IntEnum):
    CLOSED, OPEN = range(2)


class CryoLevel(IntEnum):
    UNKNOWN, TOO_LOW, NORMAL, LOW, HIGH, TOO_HIGH = range(6)


class Position(IntEnum):
    HOME, SOAK, GONIO, DEWAR, UNKNOWN = range(5)


class ModeType(IntEnum):
    MANUAL, AUTO = range(2)


class ActiveType(Enum):
    INACTIVE, ACTIVE = range(2)


class EnableType(IntEnum):
    DISABLED, ENABLED = range(2)


class ActionType(IntEnum):
    MOUNT = auto()
    DISMOUNT = auto()
    PREFETCH = auto()


class ErrorType(IntEnum):
    OK = 0
    TIMEOUT = auto()
    SAMPLE = auto()
    COLLISION = auto()
    ERROR = auto()


class OutputFlags(IntFlag):
    LID = auto()
    AUTOFILL = auto()
    HEATER = auto()
    VALVE1 = auto()
    VALVE2 = auto()
    OUTPUT05 = auto()
    OUTPUT06 = auto()
    RUNNING = auto()
    OUTPUT08 = auto()
    UNLOADING = auto()
    LOADING = auto()
    APPROACH = auto()
    OUTPUT12 = auto()
    OUTPUT13 = auto()
    TOOL = auto()
    OUTPUT15 = auto()


class AuntISARA(models.Model):
    connected = models.Enum('CONNECTED', choices=ActiveType, default=0, desc="Connection")
    enabled = models.Enum('ENABLED', choices=EnableType, default=1, desc="Control")
    status = models.Enum('STATUS', choices=StatusType, desc="Status")
    health = models.Enum('HEALTH', choices=ErrorType, desc="Health")
    log = models.String('LOG', desc="Sample Operation Message", max_length=1024)
    error = models.String('ERROR', desc="Error Message", max_length=1024)
    warning = models.String('WARNING', max_length=1024, desc='Warning message')
    help = models.String('HELP', max_length=1024, desc='Help')
    duration = models.Float('DURATION', default=0.0, units='sec', desc='Duration')

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

    output0_fbk = models.BinaryInput('STATE:OUT0', desc='Digital Outputs 00-15')
    output1_fbk = models.BinaryInput('STATE:OUT1', desc='Digital Outputs 16-31')
    output2_fbk = models.BinaryInput('STATE:OUT2', desc='Digital Outputs 32-47')
    output3_fbk = models.BinaryInput('STATE:OUT3', desc='Digital Outputs 48-63')

    # Status
    mode_fbk = models.Enum('STATE:mode', choices=ModeType, desc='Control Mode')
    error_fbk = models.Integer('STATE:error', desc='Error Code')
    position_fbk = models.String('STATE:pos', max_length=40, desc='Position')
    default_fbk = models.Enum('STATE:default', choices=OffOn, default=0, desc='Default Status')
    tool_fbk = models.Enum('STATE:tool', choices=ToolType, desc='Tool Status')
    tool_open_fbk = models.Enum('STATE:toolOpen', choices=OpenClosed, default=0, desc='Tool Open')
    path_fbk = models.String('STATE:path', max_length=40, desc='Path Name')
    puck_tool_fbk = models.Integer('STATE:toolPuck', min_val=0, max_val=29, desc='Puck On tool')
    puck_tool2_fbk = models.Integer('STATE:tool2Puck', min_val=0, max_val=29, desc='Puck On tool2')
    puck_diff_fbk = models.Integer('STATE:diffPuck', min_val=0, max_val=29, desc='Puck On Diff')
    sample_tool_fbk = models.Integer('STATE:toolSmpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On tool Sample')
    sample_tool2_fbk = models.Integer('STATE:tool2Smpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On tool2 Sample')
    sample_diff_fbk = models.Integer('STATE:diffSmpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On Diff Sample')
    plate_fbk = models.Integer('STATE:plate', min_val=0, max_val=NUM_PLATES, desc='Plate Status')
    barcode_fbk = models.String('STATE:barcode', max_length=40, desc='Barcode Status')
    power_fbk = models.Enum('STATE:power', choices=OffOn, default=0, desc='Power')
    running_fbk = models.Enum('STATE:running', choices=OffOn, default=0, desc='Path Running')
    trajectory_fbk = models.Enum('STATE:traj', choices=OffOn, default=0, desc='Traj Running')
    autofill_fbk = models.Enum('STATE:autofill', choices=OffOn, default=0, desc='LN2 Mode')
    approach_fbk = models.Enum('STATE:approach', choices=OffOn, default=0, desc='Approaching')
    magnet_fbk = models.Enum('STATE:magnet', choices=OffOn, default=0, desc='Magnet')
    heater_fbk = models.Enum('STATE:heater', choices=OffOn, default=0, desc='Heater')
    lid_fbk = models.Enum('STATE:lidOpen', choices=OpenClosed, default=0, desc='Lid')
    software_fbk = models.Enum('STATE:software', choices=OffOn, default=0, desc='Software')
    remote_speed_fbk = models.Enum('STATE:remSpeed', choices=OffOn, default=0, desc='Remote Speed')

    speed_fbk = models.Integer('STATE:speed', min_val=0, max_val=100, units='%', desc='Speed Ratio')
    pos_dew_fbk = models.Integer('STATE:posDewar', min_val=-1, max_val=30, desc='Position in Dewar')
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
    dewar_temp1_fbk = models.Float('STATE:dewarT1', prec=1, desc='Dewar Temp 1')
    dewar_temp2_fbk = models.Float('STATE:dewarT2', prec=2, desc='Dewar Temp 2')

    # Params
    next_port = models.String('PAR:nextPort', max_length=40, default='', desc='Port')
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
    power_cmd = models.Toggle('CMD:power', high=0, desc='Power')
    panic_cmd = models.Toggle('CMD:panic', desc='Panic')
    abort_cmd = models.Toggle('CMD:abort', desc='Abort')
    pause_cmd = models.Toggle('CMD:pause', desc='Pause')
    reset_cmd = models.Toggle('CMD:reset', desc='Reset')
    restart_cmd = models.Toggle('CMD:restart', desc='Restart')
    recover_cmd = models.Toggle('CMD:recover', desc='Recover')
    clear_barcode_cmd = models.Toggle('CMD:clrBarcode', desc='Clear Barcode')
    open_cmd = models.Toggle('CMD:lid:open', desc='Open Lid')
    close_cmd = models.Toggle('CMD:lid:close', desc='Close Lid')
    tool_cmd = models.Enum('CMD:tool', choices=('Close Tool', 'Open Tool'), desc='Tool')
    faster_cmd = models.Toggle('CMD:faster', desc='Speed Up')
    slower_cmd = models.Toggle('CMD:slower', desc='Speed Down')
    magnet_enable = models.Toggle('CMD:magnet', high=0, desc='Magnet')
    heater_enable = models.Toggle('CMD:heater', high=0, desc='Heater')
    speed_enable = models.Toggle('CMD:speed', high=0, desc='Remote Speed')
    approach_enable = models.Toggle('CMD:approach', high=0, desc='Approaching')
    running_enable = models.Toggle('CMD:running', high=0, desc='Path Running')
    autofill_enable = models.Toggle('CMD:autofill', high=0, desc='LN2 AutoFill')

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
    prefetch_cmd = models.Toggle('CMD:prefetch', desc='Prefetch')


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
            'tool': ToolType.DOUBLE.value,
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


def path_name(text):
    if text == 'None':
        return ''
    else:
        return text.upper()


def name_to_tool(text):
    return {
        'double': ToolType.DOUBLE.value,
        'simple': ToolType.UNIPUCK.value,
        'laser': ToolType.LASER.value,
        'flange': ToolType.NONE.value,
    }.get(text.lower(), 1)


def chain_monitors(*funcs, **kwargs) -> bool:
    """
    Chain multiple monitors together.  Each monitor will be called in order with the same arguments.
    :param funcs: functions or methods to call, each function should return a boolean to indicate success
    :param kwargs: arguments to pass to each function
    """

    for func in funcs:
        if not func(**kwargs):
            return False
    return True


class TimeoutManager(object):
    # Flag name mapping to timeout duration for error flag, by default uses DEFAULT_TIMEOUT
    FLAGS = {
        msgs.Error.AWAITING_FILL: 3600,
        msgs.Error.AWAITING_GONIO: 100,
        msgs.Error.AWAITING_LID: 100,
        msgs.Error.AWAITING_PUCK: 100,
        msgs.Error.AWAITING_SOAK: 100,
        msgs.Error.AWAITING_SAMPLE: 100,
        msgs.Error.SAMPLE_MISMATCH: 100,
        msgs.Error.PIN_MISSING: 100,
    }
    DEFAULT_TIMEOUT = 1  # all errors elapse after this duration if not explicitly specified above

    def __init__(self):
        self.elapse = defaultdict(float)

    def update(self, flags):
        """
        Update the flag timeout times, 0.0 means not active, any value above 0.0 will be the epoch after which
        the error flag matures.  This is calculated based on the FLAGS dictionary above

        :param flags: Error Flags
        """
        for flag in msgs.Error:
            if flag in flags:
                if self.elapse[flag] == 0.0:
                    self.elapse[flag] = time.time() + self.FLAGS.get(flag, self.DEFAULT_TIMEOUT)
            else:
                if self.elapse[flag] != 0.0:
                    self.elapse[flag] = 0.0

    def check(self):
        """
        Fail if any of the error flags have elapsed
        """
        for flag, elapse_after in self.elapse.items():
            if 0.0 < elapse_after < time.time():
                return flag


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
        """
        Load most recent positions from directory
        """
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
                'names': numpy.array([name for name, coords in self.raw.items() for coord in coords]),
                'coords': numpy.array([coord[:3] for name, coords in self.raw.items() for coord in coords]),
                'tolerances': numpy.array([coord[-1] for name, coords in self.raw.items() for coord in coords]),
            }
        else:
            self.info = {}

    def save(self):
        """
        Save current positions to a file. If previous file was older than today, create a new one with todays's date
        as a suffix.
        """
        logger.info('Saving positions ...')
        positions_file = '{}-{}.dat'.format(self.name, datetime.today().strftime('%Y%m%d'))
        with open(os.path.join(self.directory, positions_file), 'w') as fobj:
            json.dump(self.raw, fobj, indent=2)

    def add(self, name, coords, tolerance, replace=False):
        entry = (coords[0], coords[1], coords[2], tolerance)
        if name in self.info['names']:
            if replace:
                self.raw[name] = [entry]
            else:
                self.raw[name].append(entry)
        else:
            self.raw[name] = [entry]
        self.setup()
        self.save()

    def check(self, coords):
        """
        Determine named position from given coordinates
        :param coords: (x, y, z)
        :return: named position or '' if unknown
        """
        if self.info:
            distances = (numpy.power((self.info['coords'] - coords), 2)).sum(axis=1)
            tolerances = numpy.power(self.info['tolerances'], 2)
            within = ((tolerances - distances) >= 0)
            if any(within):
                return self.info['names'][within][0]
            else:
                return ''
        else:
            return ''


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
        self.standby_time = time.time()

        self.dewar_pucks = set()

        # Prepare connection/protocols
        self.command_client = isara.CommandFactory(self)
        self.status_client = isara.StatusFactory(self)
        self.pending_clients = {self.command_client.protocol.message_type, self.status_client.protocol.message_type}
        reactor.connectTCP(address, status_port, self.status_client)
        reactor.connectTCP(address, command_port, self.command_client)

        # status pvs and conversion types
        # maps status position to record, converter pairs
        self.status_records = {
            'power': self.ioc.power_fbk,
            'auto': self.ioc.mode_fbk,
            'default': self.ioc.default_fbk,
            'a_puck': self.ioc.puck_tool_fbk,
            'a_pin': self.ioc.sample_tool_fbk,
            'gonio_puck': self.ioc.puck_diff_fbk,
            'gonio_pin': self.ioc.sample_diff_fbk,
            'plate': self.ioc.plate_fbk,
            'barcode': self.ioc.barcode_fbk,
            'running': self.ioc.running_fbk,
            'autofill': self.ioc.autofill_fbk,
            'speed': self.ioc.speed_fbk,
            'b_puck': self.ioc.puck_tool2_fbk,
            'b_pin': self.ioc.sample_tool2_fbk,
            'soak_count': self.ioc.soak_count_fbk,
            'low_temp': self.ioc.dewar_temp1_fbk,
            'high_temp': self.ioc.dewar_temp2_fbk,
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
            OutputFlags.TOOL: self.ioc.tool_open_fbk,
            OutputFlags.APPROACH: self.ioc.approach_fbk,
            OutputFlags.LOADING | OutputFlags.UNLOADING: self.ioc.trajectory_fbk,
            OutputFlags.RUNNING: self.ioc.software_fbk,
            OutputFlags.HEATER: self.ioc.heater_fbk
        }

        self.mounting = False
        self.aborted = False

        self.positions = PositionManager(positions, self.app_directory)
        self.timeouts = TimeoutManager()

    def sender(self):
        """
        Main method which sends commands to the robot from the commands queue. New commands are placed in the queue
        to be sent through this method. This method is called within the sender thread.
        """
        self.command_on = True
        gepics.threads_init()
        while self.command_on:
            command, full_cmd = self.commands.get()
            logger.info(f'COMMAND <~ {full_cmd}')
            try:
                self.command_client.send_message(full_cmd)
            except Exception as e:
                logger.error(f'{full_cmd}: {e}')
            time.sleep(0.01)

    def status_monitor(self):
        """
        Sends status commands and receives status replies from ISARA
        """
        gepics.threads_init()
        self.status_on = True
        parsers = {
            'state': self.parse_state,
            'position': self.parse_position,
            'di2': self.parse_pucks,
            'di': self.parse_inputs,
            'do': self.parse_outputs,
            'message': self.parse_message
        }
        commands = ['state', 'di', 'di2', 'do', 'state', 'position', 'message']
        for command in cycle(commands):
            if not self.status_on:
                break
            self.status_client.send_message(command)
            success, reply = self.wait_for_queue(command, self.statuses, timeout=3)
            if success:
                parsers[command](reply)
            else:
                logger.warning(reply)
            time.sleep(STATUS_TIME)

    def response_monitor(self):
        """
        Receives responses from commands sent to ISARA.
        """
        self.command_on = True
        gepics.threads_init()
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

            command_thread = Thread(target=self.sender, daemon=True)
            status_thread = Thread(target=self.status_monitor, daemon=True)
            response_thread = Thread(target=self.response_monitor, daemon=True)

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

    def update_health(self):
        err_flag = msgs.Error(self.ioc.error_fbk.get())

        # flags for timeout error
        timeout_flags = (
                msgs.Error.AWAITING_SAMPLE |
                msgs.Error.AWAITING_GONIO |
                msgs.Error.AWAITING_LID |
                msgs.Error.AWAITING_PUCK |
                msgs.Error.AWAITING_FILL
        )
        if self.timeouts.check():
            # an error has matured
            if err_flag & timeout_flags:
                self.ioc.health.put(ErrorType.TIMEOUT)
            elif msgs.Error.SAMPLE_MISMATCH in err_flag:
                self.ioc.health.put(ErrorType.SAMPLE)
            elif msgs.Error.COLLISION in err_flag:
                self.ioc.health.put(ErrorType.COLLISION)
            elif err_flag == 0:
                self.ioc.health.put(ErrorType.OK)
            else:
                self.ioc.health.put(ErrorType.ERROR)
        else:
            self.ioc.health.put(ErrorType.OK)

    @staticmethod
    def wait_for_queue(context, queue=None, timeout=5):
        context, reply = queue.get()
        end_time = time.time() + timeout
        while time.time() < end_time and not context.startswith(context):
            time.sleep(0.05)
            context, reply = queue.get()

        if time.time() < end_time:
            return True, reply
        else:
            logger.warn(f'Timeout waiting for response "{context}"')
            return False, f'Timeout waiting for response to command "{context}"'

    def wait_for_value(
        self, variable: Any, *values: Any, timeout: int = 30, invert: bool = False, message: str = None
    ) -> bool:
        """
        Wait for a variable to reach a specific value
        :param variable: process variable to check
        :param values: values to check
        :param timeout: max duration to wait
        :param invert: if True, wait for the variable to not be in values
        :param message: optional message to use for logging
        :return: True if successful, False if timed out
        """

        if message is None:
            value_str = ' | '.join([str(v) for v in values])
            link_str = 'to be' if not invert else 'not to be'
            message = f'"{variable.name}" {link_str} "{value_str}"'

        end_time = time.time() + timeout
        logger.info(f'Waiting for {message} ...')

        while time.time() < end_time:
            current_value = variable.get()
            if invert != (current_value in values):
                break
            time.sleep(0.01)
            if self.aborted:
                return False
        else:
            logger.warn(f'Timed-out waiting for {message} after {timeout} seconds!')
            return False
        return True

    def wait_for_position(self, *positions, timeout=30):
        message = 'position ' + '|'.join(positions)
        return self.wait_for_value(self.ioc.position_fbk, *positions, timeout=timeout, message=message)

    def wait_above_coord(self, x: float = None, y: float = None, z: float = None, timeout: int =30, **kwargs) -> bool:
        """
        Wait for the robot to go below a specific coordinate
        :param x: x coordinate or None to ignore this axis
        :param y: y coordinate or None to ignore this axis
        :param z: z coordinate or None to ignore this axis
        :param timeout: max duration to wait
        :param kwargs: additional arguments
        :return: True if successful, False if timed out
        """
        end_time = time.time() + timeout
        pos = {'x': x, 'y': y, 'z': z}
        pos_text = ', '.join([f'{k}={v}' for k, v in pos.items() if v is not None])
        logger.info(f'Waiting to go below coordinates: {pos_text}')
        while time.time() < end_time:
            current_x = self.ioc.xpos_fbk.get()
            current_y = self.ioc.ypos_fbk.get()
            current_z = self.ioc.zpos_fbk.get()
            if (x is None or current_x <= x) and (y is None or current_y <= y) and (z is None or current_z <= z):
                break
            time.sleep(0.01)
            if self.aborted:
                return False
        else:
            logger.warn(f'Timed-out waiting above {pos_text} after {timeout} seconds!')
            return False
        return True

    def wait_below_coord(self, x: float = None, y: float = None, z: float = None, timeout: int = 30) -> bool:
        """
        Wait for the robot to go above a specific coordinate
        :param x: x coordinate or None to ignore this axis
        :param y: y coordinate or None to ignore this axis
        :param z: z coordinate or None to ignore this axis
        :param timeout: max duration to wait
        :return: True if successful, False if timed out
        """
        end_time = time.time() + timeout
        pos = {'x': x, 'y': y, 'z': z}
        pos_text = ', '.join([f'{k}={v}' for k, v in pos.items() if v is not None])
        logger.info(f'Waiting to go above coordinates: {pos_text}')
        while time.time() < end_time:
            current_x = self.ioc.xpos_fbk.get()
            current_y = self.ioc.ypos_fbk.get()
            current_z = self.ioc.zpos_fbk.get()
            if (x is None or current_x >= x) and (y is None or current_y >= y) and (z is None or current_z >= z):
                break
            time.sleep(0.01)
            if self.aborted:
                return False
        else:
            logger.warn(f'Timed-out below {pos_text} after {timeout} seconds!')
            return False
        return True

    # aliases for wait_for_value
    def wait_for_state(self, *states, timeout=30):
        state_values = [int(s) for s in states]
        message = 'state ' + '|'.join([state.name for state in states])
        return self.wait_for_value(self.ioc.status, *state_values, timeout=timeout, message=message)

    def wait_in_state(self, state, timeout=30):
        message = f'state to leave {state.name}'
        return self.wait_for_value(self.ioc.status, state, timeout=timeout, invert=True, message=message)

    def wait_in_path(self, path, timeout=30):
        message = f'path {path} to complete'
        return self.wait_for_value(self.ioc.path_fbk, path, timeout=timeout, invert=True, message=message)

    def ready_for_commands(self):
        return self.ready and self.ioc.enabled.get() and self.ioc.connected.get()

    @staticmethod
    def make_args(tool=0, puck=0, sample=0, puck_type=PuckType.UNIPUCK.value, x_off=0, y_off=0, z_off=0, **kwargs):
        return (tool, puck, sample) + (0,) * 4 + (puck_type, 0, 0) + (x_off, y_off, z_off)

    def send_command(self, command, *args):
        if self.ready_for_commands():
            if args:
                cmd = f'{command}({",".join([str(arg) for arg in args])})'
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

    def require_position(self, *allowed):
        logger.debug(f'Require position: {allowed} = {self.ioc.position_fbk.get()}')
        if not self.positions.is_ready():
            self.warn('No positions have been defined')
            self.ioc.help.put(
                'Please move the robot manually and save positions named `{}`'.format(' | '.join(allowed)))
            return False

        current = self.ioc.position_fbk.get()
        for pos in allowed:
            if re.match('^' + pos + r'(?:_\w*)?$', current):
                return True
        self.warn('Command allowed only from ` {} ` position'.format(' | '.join(allowed)))
        self.ioc.help.put('Please move the robot into the correct position and the re-issue the command')
        return False

    def require_tool(self, *tools):
        if self.ioc.tool_fbk.get() in [t.value for t in tools]:
            return True
        else:
            self.warn('Invalid tool for command!')

    def warn(self, msg):
        logger.warning(msg)
        self.ioc.warning.put(msg)

    def log(self, msg):
        logger.info(msg)
        self.ioc.log.put(msg)

    # parsing
    def parse_inputs(self, message):
        bitstring = message.replace(',', '').ljust(64, '0')
        inputs = [self.ioc.input0_fbk, self.ioc.input1_fbk, self.ioc.input2_fbk, self.ioc.input3_fbk]

        for pv, bits in zip(inputs, textwrap.wrap(bitstring, 16)):
            pv.put(int(bits, 2))

        for i, bit in enumerate(bitstring):
            if i in self.input_map:
                self.input_map[i].put(int(bit))
            if i in self.rev_input_map:
                self.rev_input_map[i].put((int(bit) + 1) % 2)

    def parse_pucks(self, message):
        bitstring = message.replace(',', '')
        self.ioc.pucks_fbk.put(bitstring)
        bit0, bit1 = textwrap.wrap(bitstring, 16)
        self.ioc.pucks_bit0.put(int(bit0[::-1], 2))
        self.ioc.pucks_bit1.put(int(bit1[::-1], 2))

    def parse_outputs(self, message):
        bitstring = message.replace(',', '').ljust(64, '0')
        outputs = [self.ioc.output0_fbk, self.ioc.output1_fbk, self.ioc.output2_fbk, self.ioc.output3_fbk]

        for i, (pv, bits) in enumerate(zip(outputs, textwrap.wrap(bitstring, 16))):
            value = int(bits, 2)
            pv.put(value)

            if i == 0:  # restrict to output0
                for flag, flag_pv in self.output_map.items():
                    flag_value = int(bool(flag & OutputFlags(value)))
                    if flag_pv.get() != flag_value:
                        flag_pv.put(flag_value)

    def parse_position(self, message: str):
        for i, value in enumerate(message.split(',')):
            try:
                self.position_map[i].put(float(value))
            except ValueError:
                logger.warning(f'Unable to parse position: {message}')

        coords = numpy.array([self.ioc.xpos_fbk.get(), self.ioc.ypos_fbk.get(), self.ioc.zpos_fbk.get()])

        name = self.positions.check(coords)
        self.ioc.position_fbk.put(name)

    def parse_message(self, message: str):
        """
        Update the error PV value if the message is different from the current value.
        :param message: message to process
        """
        if self.ioc.error.get() != message:
            self.ioc.error.put(message)
            if message:
                logger.warning(message)

    def parse_state(self, message):
        info = parse_text(STATE_SPECS, message)
        if not info:
            logger.warning(f'Unable to parse state: {message}')
            return

        for key, record in self.status_records.items():
            if key in info:
                record.put(info[key])

        self.ioc.tool_fbk.put(name_to_tool(info['tool']))
        self.ioc.path_fbk.put(path_name(info['path']))

        # set mounted state
        port = pin2port(info['gonio_puck'], info['gonio_pin'])
        if port != self.ioc.mounted_fbk.get():
            self.ioc.mounted_fbk.put(port)

        # set tooled state
        port = pin2port(info['a_puck'], info['a_pin'])
        if port != self.ioc.tooled_fbk.get():
            self.ioc.tooled_fbk.put(port)

        # set tooled2 state
        port = pin2port(info['b_puck'], info['b_pin'])
        if port != self.ioc.tooled2_fbk.get():
            self.ioc.tooled2_fbk.put(port)

        fault_active = False
        next_status = None

        cur_status = self.ioc.status.get()
        if fault_active:
            next_status = StatusType.FAULT
        elif self.ioc.running_fbk.get() and self.ioc.trajectory_fbk.get():
            next_status = StatusType.BUSY
        elif self.ioc.running_fbk.get() and time.time() >= self.standby_time:
            next_status = StatusType.STANDBY.value
        elif not self.ioc.running_fbk.get():
            next_status = StatusType.IDLE

        if next_status is not None and next_status != cur_status:
            self.ioc.status.put(next_status)

        # set cryo level status
        if info['high_temp'] < -170:
            self.ioc.cryo_level.put(CryoLevel.TOO_HIGH.value)
        elif info['low_temp'] > -190:
            self.ioc.cryo_level.put(CryoLevel.TOO_LOW.value)
        else:
            self.ioc.cryo_level.put(CryoLevel.NORMAL.value)

    def sample_mismatch(self) -> bool:
        """
        Check if the sample mounted state disagrees with the diffractometer state
        :return: boolelan
        """
        sample_mounted = bool(self.ioc.mounted_fbk.get())
        sample_detected = bool(self.ioc.sample_detected.get())
        return sample_mounted and (sample_detected != sample_mounted)

    def port_is_valid(self, port: str) -> bool:
        """
        Check if the port is a valid port
        :param port:
        """
        m = PORT_PATT.match(port)
        if m:
            port_info = m.groupdict()
            port_info['pin'] = int(port_info['pin'])
            puck_detected = port_info['puck'] in self.dewar_pucks
            pin_is_valid = (1 <= port_info['pin'] <= 16)
            if not puck_detected:
                self.warn(f'Port {port_info["puck"]} not in dewar')
            return puck_detected and pin_is_valid
        return False

    def set_mount_params(self, port: str):
        """
        Configure the low level mount parameters based on the port
        :param port: str
        """
        params = port2args(port)
        if params and all(params.values()):
            if params['mode'] == 'puck':
                self.ioc.tool_param.put(params['tool'])
                self.ioc.puck_param.put(params['puck'])
                self.ioc.sample_param.put(params['sample'])
            elif params['mode'] == 'plate':
                self.ioc.tool_param.put(params['tool'])
                self.ioc.plate_param.put(params['plate'])
            else:
                self.warn(f'Invalid Port parameters for mounting: {params}')
                return
            return True

    def abort_to_home(self):
        self.send_command('abort')
        is_idle = self.wait_for_state(StatusType.IDLE)
        self.send_command('reset')
        time.sleep(2)
        self.send_command('safe', self.ioc.tool_fbk.get())
        at_home = self.wait_for_position("HOME")
        is_idle = self.wait_for_state(StatusType.IDLE)
        return at_home and is_idle

    def recover_to_soak(self, sample=False):
        """
        Recover from a failed pick operation
        :param sample: if True, the sample is in the tool
        """
        logger.info('Recovering to SOAK position...')
        if self.abort_to_home():
            if self.grippers_occupied():
                pass
            elif self.pin_on_picker() or self.pin_on_placer():
                self.ioc.back_cmd.put(1)
            else:
                self.ioc.soak_cmd.put(1)
            self.wait_for_position("SOAK")
            success = self.wait_for_state(StatusType.IDLE)
            self.warn('Recovered automatically!')
        else:
            self.warn('Manual intervention required!')
            success = False
        return success

    def check_picking(self, **kwargs) -> bool:
        """
        Monitor the pick operation
        :param kwargs: keyword arguments, must contain a 'next_port' key
        """

        next_port = kwargs.get('next_port')
        if not next_port:
            logger.error('No next port provided!')
            return False

        if self.ioc.tooled_fbk.get() == next_port:
            logger.info(f'Sample already picked - {next_port}!')
            return True

        reached_dewar = self.wait_above_coord(z=DEWAR_PIN_Z)
        left_dewar = self.wait_below_coord(z=DEWAR_PIN_Z)
        time.sleep(2.0) # stabilization time
        message = 'sample to be picked'
        sample_picked  = self.wait_for_value(self.ioc.tooled_fbk,  next_port, timeout=2, message=message)

        if self.aborted:
            logger.error('Aborted!')
            return False

        flags = msgs.Error(self.ioc.error_fbk.get())
        sample_not_picked = not sample_picked
        hard_collision = not bool(self.ioc.collision_ok.get())
        soft_collision = bool(msgs.Error.COLLISION in flags)
        pin_missing = bool(msgs.Error.PIN_MISSING in flags)

        failures = {
            sample_not_picked: "Sample not picked",
            hard_collision: "Hard collision detected",
            soft_collision: "Soft collision detected",
            pin_missing: "Pin missing",
        }

        failed_pick = any(list(failures.keys()))
        failure_msg = ', '.join([msg for failed, msg in failures.items() if failed])

        if failed_pick:
            logger.error(f'Failed to pick {next_port}: {failure_msg}!')
            if hard_collision:
                self.warn(f'Manual recovery required!')
            else:
                self.recover_to_soak()
                self.warn(f'Failed to pick {next_port}!')
            return False
        return True

    def check_gonio_ready(self, **kwargs) -> bool:
        """
        Check if the gonio is ready
        :param kwargs: keyword arguments
        """

        gonio_ready = self.wait_for_value(self.ioc.gonio_ready, 1, message='gonio to be ready')
        if self.aborted:
            logger.error('Aborted!')
            return False

        flags = msgs.Error(self.ioc.error_fbk.get())
        failed_gonio = (
            (not gonio_ready) and
            (msgs.Error.AWAITING_GONIO in flags)
        )
        if failed_gonio:
            self.warn('Gonio not ready!')
            self.recover_to_soak()
            return False
        return True

    def check_soak_ready(self, **kwargs) -> bool:
        """
        Wait for operation to complete and for gripper to return to SOAK position
        :param kwargs:
        """
        soak_ready = self.wait_for_position("SOAK")
        if self.aborted:
            logger.error('Aborted!')
            return False

        if not soak_ready:
            self.warn('Gripper not in SOAK position!')
            return False
        return True

    def check_standby(self, **kwargs) -> bool:
        """
        Check if the robot is in standby mode
        :param kwargs: keyword arguments
        """
        standby = self.wait_for_state(StatusType.STANDBY)
        if self.aborted:
            logger.error('Aborted!')
            return False

        if not standby:
            self.warn('Robot failed to reach in standby state!')
            if not (self.ioc.tooled_fbk.get() or self.ioc.tooled2_fbk.get()):
                self.recover_to_soak()
            else:
                self.warn('Manual intervention required!')
                self.abort_to_home()
            return False
        return True

    def check_sample_mounted(self, **kwargs) -> bool:
        """
        Check if the sample has been mounted
        :param kwargs: keyword arguments
        """
        next_port = kwargs.get('next_port')
        if not next_port:
            logger.error('No next port provided!')
            return False

        sample_mounted = self.wait_for_value(self.ioc.mounted_fbk, next_port, message='sample to be mounted')
        time.sleep(2)  # stabilization time
        sample_on_gonio = self.wait_for_value(self.ioc.sample_detected, 1, message='sample on gonio')

        if self.aborted:
            logger.error('Aborted!')
            return False

        flags = msgs.Error(self.ioc.error_fbk.get())
        sample_not_mounted = not (sample_mounted and sample_on_gonio)
        hard_gonio_collision = not bool(self.ioc.collision_ok.get())
        soft_gonio_collision = bool(msgs.Error.COLLISION in flags)
        tool_gonio_mismatch = self.sample_mismatch()
        sample_in_tool = self.ioc.tooled_fbk.get() or self.ioc.tooled2_fbk.get()

        failures = {
            sample_not_mounted: "Gonio can't detect pin",
            tool_gonio_mismatch: "Tool and gonio states don't match",
            soft_gonio_collision: "Soft collision detected at gonio",
            hard_gonio_collision: "Hard collision detected at gonio",
        }
        failed_mount = any(list(failures.keys()))
        if failed_mount:
            message = ', '.join([msg for failed, msg in failures.items() if failed])
            logger.warn(f'Failures: {message}!')
            if hard_gonio_collision:
                msg = 'Hard collision detected at gonio!'
                logger.error(msg)
                self.warn(f'{msg}. Manual recovery required!')
            else:
                self.recover_to_soak()
                self.warn(f'Failed to mount {next_port}!')
            return False
        return True

    def check_sample_dismount(self, **kwargs) -> bool:
        """
        Monitor the dismount operation
        :param kwargs: must contain a 'cur_port' key
        """
        cur_port = kwargs.get('cur_port')
        if not cur_port:
            logger.error('No cur_port provided!')
            return False

        sample_in_tool = self.wait_for_value(
            self.ioc.tooled2_fbk, cur_port, message='sample to be dismounted', timeout=10
        )

        if self.aborted:
            logger.error('Aborted!')
            return False

        failed_dismount = (
            not sample_in_tool
        )
        if failed_dismount:
            self.recover_to_soak()
            logger.error('Failed to dismount sample!')
            return False
        return True

    def check_approach(self, **kwargs) -> bool:
        """
        Monitor the dismount operation
        :param kwargs: must contain a 'cur_port' key
        """

        approached = self.wait_for_value(self.ioc.approach_fbk, 1, message='arm to approach gonio', timeout=10)

        if self.aborted:
            logger.error('Aborted!')
            return False

        if not approached:
            self.recover_to_soak()
            logger.error('Failed to approach gonio!')
            return False
        return True

    def check_sample_returned(self, **kwargs) -> bool:
        """
        Check if the sample has been returned to the dewar
        :param kwargs: keyword arguments
        """
        sample_returned = self.wait_for_value(self.ioc.tooled2_fbk, '', message='sample to be returned', timeout=10)

        if self.aborted:
            logger.error('Aborted!')
            return False

        failed_return = (
            not sample_returned
        )
        if failed_return:
            self.recover_to_soak()
            logger.error('Failed to return sample!')
            return False
        return True

    def replace_sample(self, **kwargs) -> bool:
        """
        If one sample is present in gripper, return the sample to the dewar and return to SOAK
        :param kwargs: keyword arguments
        """
        logger.info('Returning sample to dewar ...')
        self.wait_for_state(StatusType.IDLE)
        if bool(self.ioc.tooled_fbk.get()) != bool(self.ioc.tooled2_fbk.get()):
            self.ioc.back_cmd.put(1)
            success = chain_monitors(
                self.wait_above_coord,
                self.check_standby,
                z=DEWAR_PIN_Z
            )
            is_idle = self.wait_for_state(StatusType.IDLE)
            return success and is_idle

        return True

    def retry_mount(self, **kwargs) -> bool:
        """
        Put sequence for retrying mount operation. Sends the command and then monitors
        the status of the system.
        """
        logger.info('Retrying mount operation ...')
        self.ioc.put_cmd.put(1)
        success = chain_monitors(
            self.check_picking,
            self.check_gonio_ready,
            self.check_approach,
            self.check_sample_mounted,
            self.check_standby,
            **kwargs
        )
        if success:
            logger.info('Retrying mount operation ...')
        else:
            logger.warn('Retrying mount operation failed!')

        return success

    @async_operation
    def mount_operation(self):
        """
        Perform Mount operation
        """
        self.aborted = False
        port = self.ioc.next_port.get()
        current = self.ioc.mounted_fbk.get()
        if port == current:
            self.warn(f'Sample Already mounted: {current}')
            return

        ready = self.wait_for_state(StatusType.IDLE, timeout=60)
        configured = self.set_mount_params(port)
        if ready and configured:
            logger.info(f'Mounting sample ... {port}')
            self.mounting = True
            start = time.time()
            self.standby_time = time.time() + 2.0
            if current:
                self.ioc.getput_cmd.put(1)
                success = chain_monitors(
                    self.check_picking,
                    self.check_gonio_ready,
                    self.check_approach,
                    self.check_sample_dismount,
                    self.check_sample_mounted,
                    self.check_sample_returned,
                    self.check_standby,
                    cur_port=current,
                    next_port=port,
                )
                if self.aborted:
                    self.warn('Mount operation aborted!')
                elif not success and self.is_soaking():
                    if self.grippers_occupied():
                        logger.warn('Both grippers are occupied!')
                        success = self.retry_mount(cur_port=current, next_port=port)
                    elif self.pin_on_picker():
                        success = self.replace_sample()

                    if success and self.pin_on_placer():
                        success = self.replace_sample()

            else:
                self.ioc.put_cmd.put(1)
                success = chain_monitors(
                    self.check_picking,
                    self.check_gonio_ready,
                    self.check_approach,
                    self.check_sample_mounted,
                    self.check_standby,
                    next_port=port,
                )
                if self.aborted:
                    self.warn('Mount operation aborted!')
                elif not success and self.pin_on_picker():
                    # retry mount operation
                    success = self.retry_mount(next_port=port)

            if success:
                self.warn(f'Mount operation succeeded - {port}!')
            else:
                self.warn(f'Mount operation failed - {port}!')

            self.mounting = False
            self.ioc.duration.put(time.time() - start)

    @async_operation
    def dismount_operation(self):
        """
        Perform dismount operation
        """
        self.aborted = False
        port = self.ioc.mounted_fbk.get()

        # send the command
        ready = self.wait_for_state(StatusType.IDLE, timeout=60)
        configured = self.set_mount_params(port)
        if ready and configured:
            logger.info(f'Dismounting sample ... {port}')
            self.mounting = True
            start = time.time()
            self.standby_time = time.time() + 2.0
            self.replace_sample()
            self.ioc.get_cmd.put(1)
            success = chain_monitors(
                self.check_gonio_ready,
                self.check_approach,
                self.check_sample_dismount,
                self.check_sample_returned,
                self.check_standby,
                cur_port=port,
            )
            if self.aborted:
                self.warn('Dismount operation aborted!')
            elif not success and self.pin_on_placer() and self.is_soaking():
                success = self.replace_sample()

            if success:
                self.warn(f'Dismount operation succeeded - {port}!')
            else:
                self.warn(f'Dismount operation failed - {port}!')
            self.ioc.duration.put(time.time() - start)
            self.mounting = False

    @async_operation
    def prefetch_operation(self):
        """
        Perform Prefetching operation
        """
        self.aborted = False
        port = self.ioc.next_port.get()
        current = self.ioc.mounted_fbk.get()
        if port == current:
            self.warn(f'Sample Already mounted: {current}')
            return

        ready = self.wait_for_state(StatusType.IDLE, timeout=60)
        configured = self.set_mount_params(port)
        if ready and configured:
            logger.info(f'Prefetching sample ... {port}')
            self.mounting = True
            start = time.time()
            self.standby_time = time.time() + 2.0
            self.ioc.pick_cmd.put(1)
            success = chain_monitors(
                self.check_picking,
                self.check_soak_ready,
                cur_port=current,
                next_port=port,
            )
            if self.aborted:
                self.warn('Prefetch operation aborted!')
            elif success:
                self.warn(f'Prefetch succeeded - {port}!')
            else:
                self.warn(f'Prefetch failed - {port}!')
            self.ioc.duration.put(time.time() - start)
            self.mounting = False

    @async_operation
    def abort_operation(self):
        self.abort_to_home()
        self.aborted = True

    def is_soaking(self) -> bool:
        """
        Check if the robot is in soaking position
        """
        return "SOAK" in self.ioc.position_fbk.get()

    def is_home(self) -> bool:
        """
        Check if the robot is in home position
        """
        return "HOME" in self.ioc.position_fbk.get()

    def pin_on_placer(self) -> bool:
        """
        Placer is tool B, for placing pin in dewar
        """
        return bool(self.ioc.tooled2_fbk.get())

    def pin_on_picker(self) -> bool:
        """
        Picker is tool A, for picking pin from dewar
        """
        return bool(self.ioc.tooled_fbk.get())

    def grippers_occupied(self) -> bool:
        """
        Both picker and placer are occupied
        """
        return self.pin_on_picker() and self.pin_on_placer()

    # callbacks
    def do_mount_cmd(self, pv, value, ioc):
        if value and self.require_position('SOAK') and not self.mounting:
            self.mount_operation()

    def do_dismount_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_position('SOAK') and not self.mounting:
            self.dismount_operation()


    def do_prefetch_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_position('SOAK'):
            self.prefetch_operation()

    def do_power_cmd(self, pv, value, ioc: AuntISARA):
        cmd = "on" if value else "off"
        self.send_command(cmd)

    def do_panic_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('panic')

    def do_abort_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.abort_operation()

    def do_pause_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('pause')

    def do_reset_cmd(self, pv, value, ioc):
        if value:
            self.send_command('reset')

    def do_restart_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('restart')

    def do_clear_barcode_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('clearbcrd')

    def do_open_cmd(self, pv, value, ioc: AuntISARA):
        self.send_command('openlid', ioc.tool_fbk.get())

    def do_close_cmd(self, pv, value, ioc: AuntISARA):
        self.send_command('closelid', ioc.tool_fbk.get())

    def do_tool_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('opentool')
        else:
            self.send_command('closetool')

    def do_faster_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('speedup')

    def do_slower_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('speeddown')

    def do_magnet_enable(self, pv, value, ioc: AuntISARA):
        cmd = "magneton" if value else 'magnetoff'
        self.send_command(cmd)

    def do_heater_enable(self, pv, value, ioc):
        cmd = 'heateron' if value else 'heateroff'
        self.send_command(cmd)

    def do_speed_enable(self, pv, value, ioc: AuntISARA):
        cmd = 'remotespeedon' if value else 'remotespeedoff'
        self.send_command(cmd)
        ioc.remote_speed_fbk.put(value)

    def do_autofill_enable(self, pv, value, ioc: AuntISARA):
        cmds = ('regulon', 'heateron') if value else ('reguloff', 'heateroff')
        for cmd in cmds:
            self.send_command(cmd)

    def do_home_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('home', ioc.tool_fbk.get())

    def do_change_tool_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_position('HOME'):
            if ioc.tool_param.get() != ioc.tool_fbk.get():
                self.send_command('home', ioc.tool_param.get())
            else:
                self.warn('Requested tool already present, command ignored')

    def do_safe_cmd(self, pv, value, ioc):
        if value:
            self.send_command('safe', ioc.tool_fbk.get())

    def do_put_cmd(self, pv, value, ioc: AuntISARA):
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

    def do_get_cmd(self, pv, value, ioc: AuntISARA):
        allowed_tools = (ToolType.UNIPUCK, ToolType.ROTATING, ToolType.DOUBLE, ToolType.PLATE)
        if value and self.require_position('SOAK') and self.require_tool(*allowed_tools):
            if self.ioc.tool_fbk.get() in [ToolType.UNIPUCK.value, ToolType.ROTATING.value, ToolType.DOUBLE.value]:
                cmd = 'get'
            else:
                cmd = 'getplate'
            self.send_command(cmd, ioc.tool_param.get())

    def do_getput_cmd(self, pv, value, ioc: AuntISARA):
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

    def do_recover_cmd(self, pv, value, ioc: AuntISARA):
        allowed_tools = (ToolType.UNIPUCK, ToolType.ROTATING, ToolType.DOUBLE, ToolType.PLATE)
        if value and self.require_tool(*allowed_tools) and ioc.running_fbk.get():
            pass

    def do_barcode_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_position('SOAK'):
            args = self.make_args(
                tool=ioc.tool_fbk.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            self.send_command('barcode', *args)

    def do_back_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            if bool(ioc.tooled_fbk.get()) != bool(ioc.tooled2_fbk.get()):
                self.send_command('back', ioc.tool_fbk.get())
            else:
                self.warn('Tool state not compatible with command, ignored')

    def do_soak_cmd(self, pv, value, ioc: AuntISARA):
        allowed = (ToolType.DOUBLE, ToolType.UNIPUCK, ToolType.ROTATING)
        if value and self.require_position('HOME') and self.require_tool(*allowed):
            self.send_command('soak', ioc.tool_fbk.get())

    def do_dry_cmd(self, pv, value, ioc: AuntISARA):
        allowed = (ToolType.DOUBLE, ToolType.UNIPUCK)
        if value and self.require_position('SOAK') and self.require_tool(*allowed):
            if ioc.dewar_temp1_fbk.get() <= -150:
                self.send_command('dry', ioc.tool_fbk.get())
            else:
                self.warn('Dewar temperature too high, command ignored')

    def do_pick_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_tool(ToolType.DOUBLE):
            args = self.make_args(
                tool=ToolType.DOUBLE.value, puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            self.send_command('pick', *args)

    def do_calib_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_position('HOME'):
            self.send_command('toolcal', ioc.tool_fbk.get())

    def do_teach_gonio_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_position('HOME') and self.require_tool(ToolType.LASER):
            self.send_command('teach_gonio', ToolType.LASER.value)

    def do_teach_puck_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_tool(ToolType.LASER) and self.require_position('HOME'):
            if ioc.puck_param.get():
                self.send_command('teach_puck', ToolType.LASER.value, ioc.puck_param.get())
            else:
                self.warn('Please select a puck number')

    def do_set_diff_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            current = ioc.next_port.get()
            if current.strip():
                params = port2args(current)
                self.send_command('setdiffr', params['puck'], params['sample'], ioc.type_param.get())
            else:
                self.warn('No Sample specified')

    def do_set_tool_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            port = ioc.next_port.get()
            if port:
                params = port2args(port)
                self.send_command('settool', params['puck'], params['sample'], ioc.type_param.get())
            else:
                self.warn('No Sample specified')

    def do_set_tool2_cmd(self, pv, value, ioc: AuntISARA):
        if value and self.require_tool(ToolType.DOUBLE):
            port = ioc.next_port.get()
            if port:
                params = port2args(port)
                self.send_command('settool2', params['puck'], params['sample'], ioc.type_param.get())
            else:
                self.warn('No Sample specified')

    def do_clear_cmd(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('clear memory')

    def do_reset_params(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('reset parameters')

    def do_reset_motion(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('resetMotion')

    def do_pucks_fbk(self, pv, value, ioc: AuntISARA):
        if len(value) == NUM_PUCKS:
            pucks_detected = {v[1] for v in zip(value, PUCK_LIST) if v[0] == '1'}
            added = pucks_detected - self.dewar_pucks
            removed = self.dewar_pucks - pucks_detected
            self.dewar_pucks = pucks_detected
            logger.info(f'Pucks changed: added={list(added)}, removed={list(removed)}')

            # If currently mounting a puck and it is removed abort
            on_tool = ioc.tooled_fbk.get().strip()
            on_tool2 = ioc.tooled2_fbk.get().strip()
            tooled_pucks = {on_tool[:2], on_tool2[:2]}
            if (tooled_pucks & removed) and ioc.status.get() in [StatusType.BUSY]:
                msg = 'Target puck removed while mounting. Aborting! Manual recovery required.'
                logger.error(msg)
                self.warn(msg)

    def do_save_pos_cmd(self, pv, value, ioc: AuntISARA):
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

    def do_status(self, pv, value, ioc: AuntISARA):
        if value == 0:
            if self.ioc.error_fbk.get() and self.ioc.position_fbk.get().strip() == 'SOAK':
                self.ioc.reset_cmd.put(1)

    def do_health(self, pv, value, ioc: AuntISARA):
        if value == ErrorType.TIMEOUT:
            self.ioc.log.put('Timeout during operation, recovering')

    def do_gonio_ready(self, pv, value, ioc: AuntISARA):
        error_flags = msgs.Error(self.ioc.error_fbk.get())
        if msgs.Error.AWAITING_GONIO in error_flags and value:
            error_flags &= ~msgs.Error.AWAITING_GONIO
            self.ioc.error_fbk.put(error_flags)

    def do_sample_detected(self, pv, value, ioc: AuntISARA):
        error_flags = msgs.Error(self.ioc.error_fbk.get())
        if msgs.Error.SAMPLE_MISMATCH in error_flags and not self.sample_mismatch():
            error_flags &= ~msgs.Error.SAMPLE_MISMATCH
            self.ioc.error_fbk.put(error_flags)

        if msgs.Error.AWAITING_SAMPLE in error_flags and value:
            error_flags &= ~msgs.Error.AWAITING_SAMPLE
            self.ioc.error_fbk.put(error_flags)

    def do_position_fbk(self, pv, value, ioc: AuntISARA):

        if 'DRY' in value:
            self.standby_time = time.time()

        # reset gonio wait flag if robot has moved to the gonio
        error_flags = msgs.Error(self.ioc.error_fbk.get())
        if 'SOAK' in value and msgs.Error.AWAITING_SOAK in error_flags:
            error_flags &= ~msgs.Error.AWAITING_SOAK
        self.ioc.error_fbk.put(error_flags)

    def do_error(self, pv, message, ioc: AuntISARA):
        err_flag = msgs.Error(0)
        if message:
            err_flag = msgs.parse_error(message)

        if self.ioc.status.get() == StatusType.IDLE:
            err_flag &= ~msgs.Error.AWAITING_GONIO # ignore gonio error in idle state

        if self.sample_mismatch():
            err_flag |= msgs.Error.SAMPLE_MISMATCH

        if err_flag != self.ioc.error_fbk.get():
            self.ioc.error_fbk.put(err_flag)

        self.timeouts.update(msgs.Error(self.ioc.error_fbk.get()))
        self.update_health()

    def do_low_ln2_level(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('setLowLN2', value)

    def do_high_ln2_level(self, pv, value, ioc: AuntISARA):
        if value:
            self.send_command('setHighLN2', value)

    def do_next_port(self, pv, port, ioc: AuntISARA):
        port = port.strip()
        # prefetched = self.ioc.tooled_fbk.get()
        # if prefetched and port != prefetched:
        #     self.warn(f'Sample already in the tool: {prefetched}')
        #     pv.put(prefetched)
        #
        if port and self.port_is_valid(port):
            pv.put(port)
        else:
            pv.put('')

    def do_tooled_fbk(self, pv, value, ioc: AuntISARA):
        self.ioc.next_port.put(value)

