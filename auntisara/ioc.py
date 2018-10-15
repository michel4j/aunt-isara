import time
import re
from Queue import Queue
from datetime import datetime
from threading import Thread
from enum import Enum
from twisted.internet import reactor
from softdev import epics, models, log
from . import isara

PUCK_LIST = [
    '1A', '2A', '3A', '4A', '5A',
    '1B', '2B', '3B', '4B', '5B', '6B',
    '1C', '2C', '3C', '4C','5C',
    '1D', '2D', '3D', '4D', '5D', '6D',
    '1E', '2E', '3E', '4E', '5E',
    '1F', '2F',
]

NUM_PUCKS = 29
NUM_PUCK_SAMPLES = 16
NUM_PLATES = 8
NUM_WELLS = 192
NUM_ROW_WELLS = 24
STATUS_TIME = 0.1

logger = log.get_module_logger(__name__)


class ToolType(Enum):
    FLANGE, UNIPUCK, ROTATING, PLATE, LASER, DOUBLE = range(6)


class PuckType(Enum):
    ACTOR, UNIPUCK = range(2)




class StatusType(Enum):
    IDLE, WAITING, BUSY, ERROR = range(4)


class AuntISARA(models.Model):
    connected = models.Enum('CONNECTED', choices=('Inactive', 'Active'), default=0, desc="Robot Connection")
    enabled = models.Enum('ENABLED', choices=('Disabled', 'Enabled'), default=1, desc="Robot Control")
    status = models.Enum('STATUS', choices=StatusType, desc="Robot Status")
    log = models.String('LOG', desc="Sample Operation Message", max_length=1024)
    log_alarm = models.Enum('LOG:ALARM', choices=('INFO', 'WARNING', 'ERROR'), desc="Log Level")
    warning = models.String('WARNING', max_length=40, desc='Warning message')

    # Safety flags
    approach = models.Enum('SAFETY:APPROACH', choices=('OFF', 'ON'), default=0, desc="Robot Approaching")
    prepare = models.Enum('SAFETY:PREPARE', choices=('OFF', 'ON'), default=0, desc="Prepare for Approach")

    # Status
    inputs_fbk = models.BinaryInput('STATE:inputs', desc='Digital Inputs')
    outputs_fbk = models.BinaryInput('STATE:outputs', desc='Digital Outputs')
    power_fbk = models.Enum('STATE:power', choices=('OFF', 'ON'), desc='Robot Power')
    mode_fbk = models.Enum('STATE:auto', choices=('OFF', 'ON'), desc='Auto Mode')
    default_fbk = models.Enum('STATE:default', choices=('OFF', 'ON'), desc='Default Status')
    tool_fbk = models.Enum('STATE:tool', choices=ToolType, desc='Tool Status')
    path_fbk = models.String('STATE:path', max_length=40, desc='Path Name')
    puck_tool_fbk = models.Integer('STATE:toolPuck', min_val=0, max_val=29, desc='Puck On tool')
    puck_tool2_fbk = models.Integer('STATE:tool2Puck', min_val=0, max_val=29, desc='Puck On tool2')
    puck_diff_fbk = models.Enum('STATE:diffPuck', min_val=0, max_val=29, desc='Puck On Diff')
    sample_tool_fbk = models.Integer('STATE:toolSmpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On tool Sample')
    sample_tool2_fbk = models.Integer('STATE:tool2Smpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On tool2 Sample')
    sample_diff_fbk = models.Integer('STATE:diffSmpl', min_val=0, max_val=NUM_PUCK_SAMPLES, desc='On Diff Sample')
    plate_fbk = models.Integer('STATE:plate', min_val=0, max_val=NUM_PLATES, desc='Plate Status')
    barcode_fbk = models.String('STATE:barcode', max_length=40, desc='Barcode Status')
    running_fbk = models.Enum('STATE:running', choices=('OFF', 'ON'), desc='Path Running')
    autofill_fbk = models.Enum('STATE:autofill', choices=('OFF', 'ON'), desc='Auto-Fill')
    speed_fbk = models.Integer('STATE:speed', min_val=0, max_val=100, units='%', desc='Speed Ratio')
    pos_dew_fbk = models.String('STATE:posDewar', max_length=40, desc='Position in Dewar')
    soak_count_fbk = models.Integer('STATE:soakCount', desc='Soak Count')
    pucks_fbk = models.Integer('STATE:pucks', default=0, desc='Puck Detection')
    xpos_fbk = models.Float('STATE:posX', desc='X-Position')
    ypos_fbk = models.Float('STATE:posY', desc='Y-Position')
    zpos_fbk = models.Float('STATE:posZ', desc='Z-Position')
    rxpos_fbk = models.Float('STATE:posRX', desc='RX-Position')
    rypos_fbk = models.Float('STATE:posRY', desc='RY-Position')
    rzpos_fbk = models.Float('STATE:posRZ', desc='RZ-Position')



    mounted_fbk = models.String('STATE:onDiff', max_length=40, desc='Mounted')

    #options
    plates_enabled = models.Enum('OPT:plates', choices=('Plates OFF', 'Plates ON'), default=0, desc='Plates Enabled')

    # Params
    next_param = models.String('PAR:nextPort', max_length=40, default='', desc='Port')
    barcode_param = models.Enum('PAR:barcode', choices=('OFF', 'ON'), default=0, desc='Enable Barcodes')
    tool_param = models.Enum('PAR:tool', choices=ToolType, default=2, desc='Selected Tool')
    puck_param = models.Integer('PAR:puck', min_val=0, max_val=NUM_PUCKS, default=0, desc='Selected Puck')
    sample_param = models.Integer('PAR:smpl', min_val=0, max_val=NUM_PUCK_SAMPLES, default=0, desc='Selected Sample')
    plate_param = models.Integer('PAR:plate', min_val=0, max_val=NUM_PLATES, desc='Selected Plate')
    type_param = models.Enum('PAR:puckType', choices=PuckType, default=PuckType.UNIPUCK.value, desc='puckType')

    # General Commands
    power_cmd = models.Enum('CMD:power', choices=('Power OFF', 'Power ON'), default=0, desc='Power')
    panic_cmd = models.Toggle('CMD:panic', desc='Panic')
    abort_cmd = models.Toggle('CMD:abort', desc='Abort')
    pause_cmd = models.Toggle('CMD:pause', desc='Pause')
    reset_cmd = models.Toggle('CMD:reset', desc='Reset')
    restart_cmd = models.Toggle('CMD:restart', desc='Restart')
    clear_barcode_cmd = models.Toggle('CMD:clrBarcode', desc='Clear Barcode')
    lid_cmd = models.Enum('CMD:lid', choices=('Lid CLOSED', 'Lid OPEN'), desc='Lid')
    tool_cmd = models.Enum('CMD:tool', choices=('Tool CLOSED', 'Tool OPEN'), default=0, desc='Tool')
    magnet_cmd = models.Enum('CMD:magnet', choices=('Magnet OFF', 'Magnet ON'), default=0, desc='Magnet')
    heater_cmd = models.Enum('CMD:heater', choices=('Heater OFF', 'Heater ON'), default=0, desc='Heater')
    speed_enable = models.Enum('CMD:speed', choices=('Speed OFF', 'Speed ON'), default=0, desc='Remote Speed')
    faster_cmd = models.Toggle('CMD:faster', desc='Speed Up')
    slower_cmd = models.Toggle('CMD:slower', desc='Speed Down')
    approach_enable = models.Enum('CMD:approach', choices=('Approach OFF', 'Approach ON'), default=0, desc='Approach')
    running_enable = models.Enum('CMD:running', choices=('Running OFF', 'Running ON'), default=0, desc='Running')
    autofill_cmd = models.Enum('CMD:autoFill', choices=('AutoFill OFF', 'AutoFill OFF'), default=0, desc='LN2 AutoFill')

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
    calib_cmd = models.Toggle('CMD:toolCal', desc='Pick')
    teach_gonio_cmd = models.Toggle('CMD:teachGonio', desc='Teach Gonio')
    teach_puck_cmd = models.Toggle('CMD:teachPuck', desc='Teach Puck')

    # Maintenance commands
    clear_cmd = models.Toggle('CMD:clear', desc='Clear')
    set_diff_cmd = models.Toggle('CMD:setDiffSmpl', desc='Set Diff Sample')
    set_tool_cmd = models.Toggle('CMD:setToolSmpl', desc='Set Tool Sample')
    set_tool2_cmd = models.Toggle('CMD:setTool2Smpl', desc='Set Tool 2 Sample ')
    reset_params = models.Toggle('CMD:resetParams', desc='Reset Parameters')
    reset_motion = models.Toggle('CMD:resetMotion', desc='Reset Motion')

    # Plate commands
    put_plate_cmd = models.Toggle('CMD:putPlate', desc='Put Plate')
    get_plate_cmd = models.Toggle('CMD:getPlate', desc='Get Plate')
    getput_plate_cmd = models.Toggle('CMD:getPutPlate', desc='Get Put Plate')

    # Simplified commands
    dismount_cmd = models.Toggle('CMD:dismount', desc='Dismount')
    mount_cmd = models.Toggle('CMD:mount', desc='Mount')


def port2args(port):
    # converts '1A16' to puck=1, sample=16, tool=2 for UNIPUCK where NUM_PUCK_SAMPLES = 16
    # converts 'P2' to plate=2, tool=3 for Plates
    if len(port) < 4: return {}
    args = {}
    if port.startswith('P'):
        args = {
            'tool': ToolType.PLATE.value,
            'plate': zero_int(port[1]),
            'mode': 'plate'
        }
    else:
        args = {
            'tool': ToolType.PUCK.value,
            'puck': PUCK_LIST.index(port[:2]),
            'sample': zero_int(port[3:]),
            'mode': 'puck'
        }
    return args


def pin2port(puck, sample):
    # converts puck=1, sample=16 to '1A16'
    if all((puck, sample)):
        return '{}{}'.format(PUCK_LIST[puck], sample)
    else:
        return ''


def plate2port(plate):
    # converts plate=1, to 'P1'
    if plate:
        return 'P{}'.format(plate,)
    else:
        return ''


def zero_int(text):
    try:
        return int(text)
    except ValueError:
        return 0


class AuntISARAApp(object):
    def __init__(self, device_name, address, command_port=1000, status_port=10000):
        self.ioc = AuntISARA(device_name, callbacks=self)
        self.inbox = Queue()
        self.outbox = Queue()
        self.send_on = False
        self.recv_on = False
        self.user_enabled = False
        self.ready = False
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
            3: (self.ioc.tool_fbk, zero_int),
            4: (self.ioc.path_fbk, str),
            5: (self.ioc.puck_tool_fbk, zero_int),
            6: (self.ioc.sample_tool_fbk, zero_int),
            7: (self.ioc.puck_diff_fbk, zero_int),
            8: (self.ioc.sample_diff_fbk, zero_int),
            9: (self.ioc.plate_fbk, zero_int),
            11: (self.ioc.barcode_fbk, str),
            12: (self.ioc.running_fbk, int),
            13: (self.ioc.autofill_fbk, int),
            15: (self.ioc.speed_fbk, int),
            18: (self.ioc.pos_dew_fbk, zero_int),
            20: (self.ioc.puck_tool2_fbk, zero_int),
            21: (self.ioc.sample_tool2_fbk, zero_int),
            22: (self.ioc.soak_count_fbk, int),
        }
        self.position_map = [
            self.ioc.xpos_fbk, self.ioc.ypos_fbk, self.ioc.zpos_fbk,
            self.ioc.rxpos_fbk, self.ioc.rypos_fbk, self.ioc.rzpos_fbk
        ]

    def ready_for_commands(self):
        return self.ready and self.ioc.enabled.get() and self.ioc.connected.get()

    def sender(self):
        self.send_on = True
        epics.threads_init()
        while self.send_on:
            command = self.outbox.get()
            logger.debug('< {}'.format(command))
            try:
                self.command_client.send_message(command)
            except Exception as e:
                logger.error(e)
            time.sleep(0)

    def receiver(self):
        self.recv_on = True
        epics.threads_init()
        while self.recv_on:
            message, message_type = self.inbox.get()
            logger.debug('> {}'.format(message))
            try:
                self.process_message(message, message_type)
            except Exception as e:
                logger.error(e)
            time.sleep(0)

    def status_monitor(self):
        epics.threads_init()
        self.recv_on = True
        commands = ['state', 'di', 'di2', 'do', 'position', 'config', 'message']
        cmd_index = 0
        while self.recv_on:
            self.status_client.send_message(commands[cmd_index])
            cmd_index = (cmd_index + 1) % len(commands)
            time.sleep(STATUS_TIME)

    def disconnect(self, client_type):
        self.pending_clients.add(client_type)
        self.recv_on = False
        self.send_on = False
        self.ioc.connected.put(0)

    def connect(self, client_type):
        self.pending_clients.remove(client_type)

        # all clients connected
        if not self.pending_clients:
            self.inbox.queue.clear()
            self.outbox.queue.clear()
            send_thread = Thread(target=self.sender)
            recv_thread = Thread(target=self.receiver)
            status_thread = Thread(target=self.status_monitor)
            send_thread.setDaemon(True)
            recv_thread.setDaemon(True)
            status_thread.setDaemon(True)
            send_thread.start()
            recv_thread.start()
            status_thread.start()
            self.ready = True
            self.ioc.connected.put(1)
            logger.warn('Controller ready!')
        else:
            self.ready = False

    def shutdown(self):
        logger.warn('Shutting down ...')
        self.recv_on = False
        self.send_on = False
        self.ioc.shutdown()

    @staticmethod
    def make_args(tool=0, puck=0, sample=0, puck_type=PuckType.UNIPUCK.value, x_off=0, y_off=0, z_off=0, **kwargs):
        return (tool, puck, sample) + (0,)*4 + (puck_type,0,0) + (x_off, y_off, z_off)

    def send_command(self, command, *args):
        if self.ready_for_commands():
            if args:
                cmd = '{}({})'.format(command, ','.join([str(arg) for arg in args]))
            else:
                cmd = command
            self.outbox.put(cmd)

    def receive_message(self, message, message_type):
        self.inbox.put((message, message_type))

    def process_message(self, message, message_type):
        if message_type == isara.MessageType.STATUS:
            # process state messages
            self.parse_status(message)
        else:
            # process response messages
            self.ioc.log.put(message)

    def parse_status(self, message):
        patt = re.compile('^(?P<context>\w+)\((?P<msg>.*?)\)')
        m = patt.match(message)
        if m:
            details = m.groupdict()
            if details['context'] == 'state':
                for i, value in enumerate(details['msg'].split(',')):
                    if i not in self.status_map: continue
                    record,  converter = self.status_map[i]
                    try:
                        record.put(converter(value))
                    except ValueError:
                        logger.warning('Unable to parse state: {}'.format(message))
                if self.ioc.mode_fbk.get() == 1 and self.ioc.default_fbk.get() == 1:
                    if self.ioc.running_fbk.get():
                        self.ioc.status.put(StatusType.BUSY.value)
                    else:
                        self.ioc.status.put(StatusType.IDLE.value)
                else:
                    self.ioc.status.put(StatusType.ERROR.value)
            elif details['context'] == 'position':
                for i, value in enumerate(details['msg'].split(',')):
                    try:
                        self.position_map[i].put(float(value))
                    except ValueError:
                        logger.warning('Unable to parse position: {}'.format(message))
            elif details['context'] == 'do':
                # process outputs
                pass
            elif details['context'] == 'di':
                bits = details['msg'].replace(',', '')
                if len(bits) == 29:  # puck detection
                    self.ioc.pucks_fbk.put(int(bits.rjust(32,'0'), 2))
                else:
                    pass
                    # process other inputs
            elif details['context'] == 'message':
                self.ioc.log.put(details['msg'])



    # callbacks
    def do_mount_cmd(self, pv, value, ioc):
        if value:
            port = ioc.next_param.get().strip()
            current = ioc.mounted_fbk.get().strip()
            params = port2args(port)
            if all(params.values()):
                if params['mode'] == 'puck':
                    command = 'put' if not current else 'getput'
                    args = self.make_args(**params)
                elif params['mode'] == 'plate':
                    command = 'putplate' if not current else 'getputplate'
                    args = (params['tool'], 0,0,0,0, params['plate'])
                else:
                    return
                self.send_command(command, *args)

    def do_dismount_cmd(self, pv, value, ioc):
        if value:
            current = ioc.mounted_fbk.get().strip()
            params = port2args(current)
            if all(params.values()):
                if params['mode'] == 'puck':
                    command = 'get'
                elif params['mode'] == 'plate':
                    command = 'getplate'
                else:
                    return
                self.send_command(command, params['tool'])

    def do_power_cmd(self, pv, value, ioc):
        if value :
            self.send_command('on')
        else:
            self.send_command('off')

    def do_panic_cmd(self, pv, value, ioc):
        if value :
            self.send_command('panic')

    def do_abort_cmd(self, pv, value, ioc):
        if value :
            self.send_command('abort')

    def do_pause_cmd(self, pv, value, ioc):
        if value :
            self.send_command('pause')

    def do_reset_cmd(self, pv, value, ioc):
        if value :
            self.send_command('reset')

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

    def do_magnet_cmd(self, pv, value, ioc):
        if value:
            self.send_command('magneton')
        else:
            self.send_command('magnetoff')

    def do_heater_cmd(self, pv, value, ioc):
        if value:
            self.send_command('heateron')
        else:
            self.send_command('heateroff')

    def do_speed_enable(self, pv, value, ioc):
        if value:
            self.send_command('remotespeedon')
        else:
            self.send_command('remotespeedoff')

    def do_faster_cmd(self, pv, value, ioc):
        if value:
            self.send_command('speedup')

    def do_slower_cmd(self, pv, value, ioc):
        if value:
            self.send_command('speeddown')

    def do_approach_enable(self, pv, value, ioc):
        if value:
            self.send_command('cryoON', ioc.tool_param.get())
        else:
            self.send_command('cryoOFF', ioc.tool_param.get())

    def do_running_enable(self, pv, value, ioc):
        if value:
            self.send_command('trajON', ioc.tool_param.get())
        else:
            self.send_command('trajOFF', ioc.tool_param.get())

    def do_home_cmd(self, pv, value, ioc):
        if value :
            self.send_command('home', ioc.tool_param.get())

    def do_safe_cmd(self, pv, value, ioc):
        if value :
            self.send_command('safe', ioc.tool_param.get())

    def do_put_cmd(self, pv, value, ioc):
        if value:
            args = self.make_args(
                tool=ioc.tool_param.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            if ioc.barcode_param.get():
                self.send_command('put_bcrd', *args)
            else:
                self.send_command('put', *args)

    def do_get_cmd(self, pv, value, ioc):
        if value:
            self.send_command('get', ioc.tool_param.get())

    def do_getput_cmd(self, pv, value, ioc):
        if value:
            args = self.make_args(
                tool=ioc.tool_param.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            if ioc.barcode_param.get():
                self.send_command('getput_bcrd', *args)
            else:
                self.send_command('getput', *args)

    def do_barcode_cmd(self, pv, value, ioc):
        if value:
            args = self.make_args(
                tool=ioc.tool_param.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            self.send_command('barcode', *args)

    def do_back_cmd(self, pv, value, ioc):
        if value:
            self.send_command('back', ioc.tool_param.get())

    def do_soak_cmd(self, pv, value, ioc):
        if value:
            self.send_command('soak', ioc.tool_param.get())

    def do_dry_cmd(self, pv, value, ioc):
        if value:
            self.send_command('dry', ioc.tool_param.get())

    def do_pick_cmd(self, pv, value, ioc):
        if value:
            args = self.make_args(
                tool=ioc.tool_param.get(), puck=ioc.puck_param.get(), sample=ioc.sample_param.get(),
                puck_type=ioc.type_param.get()
            )
            self.send_command('pick', *args)

    def do_calib_cmd(self, pv, value, ioc):
        if value :
            self.send_command('toolcal', ioc.tool_param.get())

    def do_teach_gonio_cmd(self, pv, value, ioc):
        if value :
            self.send_command('teach_gonio', ToolType.LASER)

    def do_teach_puck_cmd(self, pv, value, ioc):
        if value :
            self.send_command('teach_puck', ToolType.LASER)

    def do_autofill_cmd(self, pv, value, ioc):
        if value:
            self.send_command('regulon')
        else:
            self.send_command('reguloff')

    def do_set_diff_cmd(self, pv, value, ioc):
        if value:
            self.send_command('setdiffr', ioc.puck_param.get(), ioc.sample_param.get(), ioc.type_param.get())

    def do_set_tool_cmd(self, pv, value, ioc):
        if value:
            self.send_command('settool', ioc.puck_param.get(), ioc.sample_param.get(), ioc.type_param.get())

    def do_set_tool2_cmd(self, pv, value, ioc):
        if value:
            self.send_command('settool2', ioc.puck_param.get(), ioc.sample_param.get(), ioc.type_param.get())

    def do_clear_cmd(self, pv, value, ioc):
        if value :
            self.send_command('clear memory')

    def do_reset_params(self, pv, value, ioc):
        if value :
            self.send_command('reset parameters')

    def do_reset_motion(self, pv, value, ioc):
        if value :
            self.send_command('resetMotion')

    def do_put_plate_cmd(self, pv, value, ioc):
        if ioc.plates_enabled.get() and value:
            self.send_command('putplate', ToolType.PLATE.value, 0, 0, 0, 0, ioc.plate_param.get())

    def do_get_plate_cmd(self, pv, value, ioc):
        if ioc.plates_enabled.get() and value:
            self.send_command('getplate', ToolType.PLATE.value)

    def do_getput_plate_cmd(self, pv, value, ioc):
        if ioc.plates_enabled.get() and value:
            self.send_command('getputplate', ToolType.PLATE.value, 0, 0, 0, 0, ioc.plate_param.get())

    def do_sample_diff_fbk(self, pv, value, ioc):
        port = pin2port(ioc.puck_diff_fbk.get(), value)
        ioc.mounted_fbk.put(port)

    def do_plate_fbk(self, pv, value, ioc):
        if value and ioc.plates_enabled.get():
            port = plate2port(value)
            ioc.tooled_fbk.put(port)
