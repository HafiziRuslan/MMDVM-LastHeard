#!/usr/bin/python3
"""MMDVM LastHeard - Telegram bot to monitor the last transmissions of a MMDVM gateway"""

import asyncio
import configparser
import datetime as dt
import difflib
import glob
import logging
import logging.handlers
import os
import re
import shutil
import signal
import subprocess
import tomllib
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Optional

import humanize
from dotenv import load_dotenv
from codes import COUNTRY_CODES, MCC_CODES
from telegram.ext import Application as TelegramApplication
from telegram.ext import ApplicationBuilder

TG_BOTTOKEN: str = ''
TG_CHATID: str = ''
TG_TOPICID: str = ''
GW_IGNORE_TIME_MESSAGES: bool = True
TG_APP: Optional[TelegramApplication] = None
MESSAGE_QUEUE: Optional[asyncio.Queue] = None
RELEVANT_LOG_PATTERNS = [
	'end of voice transmission',
	'end of transmission',
	'watchdog has expired',
	# 'received RF data',
	# 'received network data',
	# 'data transmission',
]


@lru_cache
def get_app_metadata():
	repo_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
	git_sha = 'unknown'
	if shutil.which('git'):
		try:
			git_sha = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD^'], cwd=repo_path).decode('ascii').strip()
		except Exception:
			pass
	meta = {'name': 'MMDVM_LastHeard', 'version': '0.0.0', 'github': 'https://github.com/HafiziRuslan/MMDVM-LastHeard'}
	try:
		with open(os.path.join(repo_path, 'pyproject.toml'), 'rb') as f:
			data = tomllib.load(f).get('project', {})
			meta.update({k: data.get(k, meta[k]) for k in ['name', 'version']})
			meta['github'] = data.get('urls', {}).get('github', meta['github'])
	except Exception as e:
		logging.warning('Failed to load project metadata: %s', e)
	return f'{meta["name"]}-{meta["version"]}-{git_sha}', meta['github']


APP_NAME, PROJECT_URL = get_app_metadata()


def configure_logging():
	log_dir = '/var/log/mmdvmlhbot'
	if not os.path.exists(log_dir) or not os.access(log_dir, os.W_OK):
		log_dir = 'logs'
	if not os.path.exists(log_dir):
		os.makedirs(log_dir)
	logging.getLogger('asyncio').setLevel(logging.DEBUG)
	logging.getLogger('hpack').setLevel(logging.DEBUG)
	logging.getLogger('httpx').setLevel(logging.DEBUG)
	logging.getLogger('telegram').setLevel(logging.DEBUG)
	logging.getLogger('urllib3').setLevel(logging.DEBUG)

	class ISO8601Formatter(logging.Formatter):
		def formatTime(self, record, datefmt=None):
			return dt.datetime.fromtimestamp(record.created, dt.timezone.utc).astimezone().isoformat(timespec='milliseconds')

	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)
	formatter = ISO8601Formatter('%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(name)s.%(funcName)s:%(lineno)d | %(message)s')
	console_handler = logging.StreamHandler()
	console_handler.setLevel(logging.WARNING)
	console_handler.setFormatter(formatter)
	logger.addHandler(console_handler)

	class NumberedRotatingFileHandler(logging.handlers.RotatingFileHandler):
		"""RotatingFileHandler with backup number before the extension."""

		def doRollover(self):
			"""Do a rollover, with numbering before the extension."""
			if self.stream:
				self.stream.close()
				self.stream = None
			if self.backupCount > 0:
				name, ext = os.path.splitext(self.baseFilename)
				for i in range(self.backupCount - 1, 0, -1):
					sfn = self.rotation_filename(f'{name}{i}{ext}')
					dfn = self.rotation_filename(f'{name}{i + 1}{ext}')
					if os.path.exists(sfn):
						if os.path.exists(dfn):
							os.remove(dfn)
						os.rename(sfn, dfn)
				dfn = self.rotation_filename(f'{name}1{ext}')
				if os.path.exists(dfn):
					os.remove(dfn)
				self.rotate(self.baseFilename, dfn)
			if not self.delay:
				self.stream = self._open()

	class LevelFilter(logging.Filter):
		def __init__(self, level):
			self.level = level

		def filter(self, record):
			return record.levelno == self.level

	levels = {
		logging.DEBUG: 'debug.log',
		logging.INFO: 'info.log',
		logging.WARNING: 'warning.log',
		logging.ERROR: 'error.log',
		logging.CRITICAL: 'critical.log',
	}
	for level, filename in levels.items():
		try:
			handler = NumberedRotatingFileHandler(os.path.join(log_dir, filename), maxBytes=1 * 1024 * 1024, backupCount=5)
			handler.setLevel(level)
			handler.addFilter(LevelFilter(level))
			handler.setFormatter(formatter)
			logger.addHandler(handler)
		except (OSError, PermissionError) as e:
			logging.error('Failed to create %s: %s', filename, e)


def load_env_variables():
	"""Load environment variables from .env file."""
	load_dotenv()
	global TG_BOTTOKEN, TG_CHATID, TG_TOPICID, GW_IGNORE_TIME_MESSAGES
	TG_BOTTOKEN = os.getenv('TG_BOTTOKEN', '')
	TG_CHATID = os.getenv('TG_CHATID', '')
	TG_TOPICID = os.getenv('TG_TOPICID', '0')
	GW_IGNORE_TIME_MESSAGES = os.getenv('GW_IGNORE_MESSAGES', 'True').lower() == 'true'
	if not TG_BOTTOKEN:
		logging.warning('TG_BOTTOKEN is not set in the environment variables.')
	if not TG_CHATID:
		logging.warning('TG_CHATID is not set in the environment variables.')
	if GW_IGNORE_TIME_MESSAGES:
		logging.info('GW_IGNORE_MESSAGES is set to true, messages from the gateway will be ignored.')
	logging.info('Environment variables loaded successfully.')


def remove_double_spaces(text: str) -> str:
	"""Removes double spaces from a string."""
	while '  ' in text:
		text = text.replace('  ', ' ')
	return text


@lru_cache(maxsize=128)
def get_country_code(country_name: str) -> str:
	"""Returns the country code for a given country name."""
	code = COUNTRY_CODES.get(country_name)
	if not code:
		for name, c in COUNTRY_CODES.items():
			if name.lower() == country_name.lower():
				code = c
				break
		if not code:
			matches = difflib.get_close_matches(country_name, COUNTRY_CODES.keys(), n=1, cutoff=0.8)
			if matches:
				code = COUNTRY_CODES[matches[0]]
	return code if code else ''


def get_flag_emoji(country_code: str) -> str:
	"""Converts a two-letter country code to a flag emoji."""
	if country_code and len(country_code) == 2:
		return ''.join(chr(ord(c) + 127397) for c in country_code.upper())
	return '🌐'


def read_talkgroup_file(file_path: str, delimiter: str, id_idx: int, name_idx: int, tg_map: dict, suffix: str = '', overwrite: bool = True):
	"""Helper to read a talkgroup file and update the map."""
	if not os.path.isfile(file_path):
		return
	try:
		with open(file_path, 'r', encoding='UTF-8', errors='replace') as file:
			for line in file:
				line = line.strip()
				if line.startswith('#') or not line:
					continue
				parts = line.split(maxsplit=1) if delimiter == ' ' else line.split(delimiter)
				try:
					if len(parts) > max(id_idx, name_idx):
						tgid = parts[id_idx].strip()
						name = parts[name_idx].strip()
						if tgid and name:
							display_name = f'{suffix}: {name}' if suffix else name
							if overwrite or tgid not in tg_map:
								tg_map[tgid] = display_name
				except IndexError:
					continue
	except Exception as e:
		logging.error('Error reading talkgroup file %s: %s', file_path, e)


_TALKGROUP_CACHE = {'mtimes': {}, 'tg_map': {}, 'networks': []}


def get_talkgroup_ids() -> dict:
	"""Reads and caches the talkgroup list from files, reloading if files change."""
	global _TALKGROUP_CACHE
	file_configs = [
		('/usr/local/etc/TGList_BM.txt', ';', 0, 2),
		('/usr/local/etc/TGList_TGIF.txt', ';', 0, 1),
		('/usr/local/etc/TGList_FreeStarIPSC.txt', ',', 0, 1),
		('/usr/local/etc/TGList_SystemX.txt', ',', 0, 1),
		('/usr/local/etc/TGList_FreeDMR.txt', ',', 0, 1),
		('/usr/local/etc/TGList_ADN.txt', ',', 0, 1),
		('/usr/local/etc/TGList_DMRp.txt', ',', 0, 1),
		('/usr/local/etc/TGList_QuadNet.txt', ',', 0, 1),
		('/usr/local/etc/TGList_AmComm.txt', ',', 0, 1),
		('/usr/local/etc/TGList_NXDN.txt', ';', 0, 1),
		('/usr/local/etc/TGList_P25.txt', ';', 0, 1),
		('/usr/local/etc/groups.txt', ' ', 0, 1),
	]
	get_dmrgateway_rules()
	dmr_networks = _DMRGATEWAY_CACHE.get('networks', [])
	for net in dmr_networks:
		name_clean = net.split('_')[0]
		fpath = f'/usr/local/etc/TGList_{name_clean}.txt'
		# BM usually uses index 2 for name, others 1
		name_idx = 2 if 'BM' in name_clean else 1
		file_configs.append((fpath, ';', 0, name_idx))
	current_mtimes = {}
	expanded_configs = []
	for pattern, delimiter, id_idx, name_idx in file_configs:
		files = glob.glob(pattern)
		expanded_configs.append((files, delimiter, id_idx, name_idx))
		for f in files:
			try:
				current_mtimes[f] = os.path.getmtime(f)
			except OSError:
				pass
	catch_all_files = glob.glob('/usr/local/etc/TGList_*.txt')
	for f in catch_all_files:
		try:
			current_mtimes[f] = os.path.getmtime(f)
		except OSError:
			pass
	if current_mtimes == _TALKGROUP_CACHE['mtimes'] and _TALKGROUP_CACHE['tg_map']:
		return _TALKGROUP_CACHE['tg_map']
	tg_map = {}
	processed_files = set()
	for files, delimiter, id_idx, name_idx in expanded_configs:
		for tg_file in files:
			processed_files.add(tg_file)
			filename = os.path.basename(tg_file)
			name_part = os.path.splitext(filename)[0]
			suffix = name_part[7:] if name_part.startswith('TGList_') else name_part
			read_talkgroup_file(tg_file, delimiter, id_idx, name_idx, tg_map, suffix=suffix, overwrite=True)
	for tg_file in catch_all_files:
		if tg_file not in processed_files:
			filename = os.path.basename(tg_file)
			name_part = os.path.splitext(filename)[0]
			suffix = name_part[7:] if name_part.startswith('TGList_') else name_part
			read_talkgroup_file(tg_file, ';', 0, 1, tg_map, suffix=suffix, overwrite=False)
	dmr_rules = _DMRGATEWAY_CACHE.get('rules', [])
	for rule in dmr_rules:
		if rule.get('type') == 'TG':
			for target_tg, label in [(4000, 'Disconnect'), (9990, 'Parrot')]:
				src_tg = target_tg - rule['offset']
				if rule['start'] <= src_tg <= rule['end']:
					tg_map[str(src_tg)] = label
	for mcc in MCC_CODES:
		country, _ = MCC_CODES[mcc]
		tg_map[f'{mcc}990'] = f'{country} Text Message'
		tg_map[f'{mcc}997'] = f'{country} Parrot'
		tg_map[f'{mcc}999'] = f'{country} ARS/RRS/GPS'
	_TALKGROUP_CACHE = {'mtimes': current_mtimes, 'tg_map': tg_map}
	return tg_map


_DMRGATEWAY_CACHE = {'path': None, 'mtime': 0, 'rules': [], 'networks': []}


def get_dmrgateway_rules() -> list:
	"""Reads DMRGateway configuration to find rewrite rules, reloading if file changes."""
	global _DMRGATEWAY_CACHE
	conf_files = ['/etc/dmrgateway', '/etc/DMRGateway.ini', '/opt/DMRGateway/DMRGateway.ini']
	config_path = _DMRGATEWAY_CACHE['path']
	if not config_path or not os.path.isfile(config_path):
		config_path = None
		for f in conf_files:
			if os.path.isfile(f):
				config_path = f
				break
	if config_path:
		try:
			mtime = os.path.getmtime(config_path)
			if config_path == _DMRGATEWAY_CACHE['path'] and mtime == _DMRGATEWAY_CACHE['mtime']:
				return _DMRGATEWAY_CACHE['rules']
			rules = []
			networks = []
			config = configparser.ConfigParser(strict=False, interpolation=None)
			config.read(config_path)
			for section in config.sections():
				if section.startswith('DMR Network'):
					net_name = config.get(section, 'Name', fallback=section)
					networks.append(net_name)
					for key, value in config.items(section):
						key_lower = key.lower()
						rule_type = None
						if key_lower.startswith('tgrewrite'):
							rule_type = 'TG'
						elif key_lower.startswith('pcrewrite'):
							rule_type = 'PC'
						if rule_type:
							parts = [p.strip() for p in value.split(',')]
							if len(parts) >= 5:
								try:
									src_slot = int(parts[0])
									src_tg = int(parts[1])
									dst_tg = int(parts[3])
									range_val = int(parts[4])
									rules.append(
										{
											'slot': src_slot,
											'start': src_tg,
											'end': src_tg + range_val - 1,
											'offset': dst_tg - src_tg,
											'name': net_name,
											'type': rule_type,
										}
									)
								except ValueError:
									continue
			_DMRGATEWAY_CACHE = {'path': config_path, 'mtime': mtime, 'rules': rules, 'networks': networks}
			return rules
		except Exception as e:
			logging.error('Error reading DMRGateway config %s: %s', config_path, e)
	return _DMRGATEWAY_CACHE['rules']


@lru_cache(maxsize=1)
def get_user_csv_data() -> dict:
	"""Reads and caches the user.csv file."""
	user_map = {}
	caller_file = '/usr/local/etc/user.csv'
	if os.path.isfile(caller_file):
		encodings = ['utf-8', 'latin-1']
		for encoding in encodings:
			try:
				temp_map = {}
				with open(caller_file, 'r', encoding=encoding) as file:
					for line in file:
						parts = line.strip().split(',')
						if len(parts) >= 7:
							call = parts[1].strip()
							fname = parts[2].strip()
							country = parts[6].strip()
							temp_map[call] = (fname, country)
				user_map = temp_map
				break
			except UnicodeDecodeError:
				continue
			except Exception as e:
				logging.error('Error reading caller file %s: %s', caller_file, e)
				break
	return user_map


@lru_cache
def get_mmdvm_log_dir() -> str:
	"""Reads the MMDVMHost configuration to find the log directory."""
	conf_files = ['/etc/mmdvmhost', '/etc/MMDVM.ini', '/opt/MMDVMHost/MMDVM.ini']
	for conf_file in conf_files:
		if os.path.isfile(conf_file):
			try:
				config = configparser.ConfigParser()
				config.read(conf_file)
				if config.has_section('Log') and config.has_option('Log', 'FilePath'):
					log_dir = config.get('Log', 'FilePath')
					if os.path.isdir(log_dir):
						return log_dir
			except Exception:
				pass
	default_dirs = ['/var/log/pi-star', '/var/log/mmdvm', '/var/log/MMDVMHost']
	for log_dir in default_dirs:
		if os.path.isdir(log_dir):
			return log_dir
	return '/var/log/pi-star'


def get_latest_mmdvm_log_path() -> Optional[str]:
	"""Finds and returns the path to the most recent MMDVM log file."""
	logdir = get_mmdvm_log_dir()
	log_files = glob.glob(os.path.join(logdir, 'MMDVM-*.log'))
	if not log_files:
		return None
	log_files.sort(key=os.path.getmtime, reverse=True)
	latest_log = log_files[0]
	logging.debug('Latest MMDVM log file: %s', latest_log)
	return latest_log


def get_last_line_of_file(file_path: str) -> str:
	"""Reads the last line of a file using seek for performance."""
	try:
		with open(file_path, 'rb') as f:
			try:
				f.seek(-4096, os.SEEK_END)
			except OSError:
				f.seek(0)
			lines = f.readlines()
			for line in reversed(lines):
				decoded = line.decode('utf-8', errors='replace').strip()
				if len(decoded) >= 10:
					return decoded
	except OSError as e:
		logging.error('Error reading last line of file %s: %s', file_path, e)
	return ''


@dataclass
class MMDVMLogLine:
	timestamp: Optional[datetime] = None
	mode: str = ''
	callsign: str = ''
	destination: str = ''
	data_type: str = ''
	block: int = 0
	duration: float = 0.0
	packet_loss: int = 0
	ber: float = 0.0
	rssi: str = 'S0'
	rssi1: int = 0
	rssi2: int = 0
	rssi3: int = 0
	url: str = ''
	slot: int = 2
	is_voice: bool = True
	is_kerchunk: bool = False
	is_network: bool = True
	is_watchdog: bool = False
	_TIMESTAMP = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)'
	_SOURCE = r'(?P<source>network|RF)'
	_CALLSIGN = r'from (?P<callsign>[\w\d\-/]+)'
	_DMR_DESTINATION = r'to (?P<destination>(?:TG [\d\w]+)|[\d\w]+)'
	_DSTAR_CALLSIGN = r'from (?P<callsign>[\w\d\s/]+)'
	_DSTAR_DESTINATION = r'to (?P<destination>[\w\d\s]+)'
	_YSF_DESTINATION = r'to DG-ID (?P<dgid>\d+)'
	_DURATION = r'(?P<duration>[\d\.]+) seconds'
	_PACKET_LOSS = r'(?P<packet_loss>[\d\.]+)% packet loss'
	_BER = r'BER: (?P<ber>[\d\.]+)%'
	_RSSI = r'RSSI: (?P<rssi1>-[\d]+)/(?P<rssi2>-[\d]+)/(?P<rssi3>-[\d]+) dBm'
	DMR_GW_PATTERN = re.compile(
		rf'^M: {_TIMESTAMP} DMR Slot (?P<slot>\d), received (?P<source>network) '
		r'(?:late entry|voice header|end of voice transmission) '
		rf'{_CALLSIGN} {_DMR_DESTINATION}'
		rf'(?:, {_DURATION}, {_PACKET_LOSS}, {_BER})'
	)
	DMR_RF_PATTERN = re.compile(
		rf'^M: {_TIMESTAMP} DMR Slot (?P<slot>\d), received (?P<source>RF) '
		r'(?:late entry|voice header|end of voice transmission) '
		rf'{_CALLSIGN} {_DMR_DESTINATION}'
		rf'(?:, {_DURATION}, {_BER}, {_RSSI})'
	)
	# DMR_DATA_PATTERN = re.compile(
	# 	rf'^M: {_TIMESTAMP} DMR Slot (?P<slot>\d), received {_SOURCE} data header from '
	# 	rf'{_CALLSIGN} to {_DMR_DESTINATION}, (?P<block>[\d]+) blocks'
	# )
	DSTAR_PATTERN = re.compile(
		rf'^M: {_TIMESTAMP} D-Star, (?:received )?{_SOURCE} end of transmission '
		rf'{_DSTAR_CALLSIGN} {_DSTAR_DESTINATION}'
		rf'(?:, | , ){_DURATION},\s+{_PACKET_LOSS}, {_BER}'
	)
	DSTAR_WATCHDOG_PATTERN = re.compile(
		rf'^M: {_TIMESTAMP} D-Star, {_SOURCE} watchdog has expired, '
		rf'{_DURATION},\s+{_PACKET_LOSS}, {_BER}'
	)
	YSF_PATTERN = re.compile(
		rf'^M: {_TIMESTAMP} YSF, received {_SOURCE} end of transmission '
		rf'{_CALLSIGN} {_YSF_DESTINATION}, '
		rf'{_DURATION}, {_PACKET_LOSS}, {_BER}'
	)
	YSF_NETWORK_DATA_PATTERN = re.compile(
		rf'^M: {_TIMESTAMP} YSF, received network data '
		rf'{_CALLSIGN}\s+{_YSF_DESTINATION} at (?P<location>\S+)'
	)

	@classmethod
	def from_logline(cls, logline: str) -> 'MMDVMLogLine':
		"""Factory method to create an MMDVMLogLine instance from a log line."""
		parsers = [
			cls._parse_dmr_voice,
			# cls._parse_dmr_data,
			cls._parse_dstar,
			cls._parse_dstar_watchdog,
			cls._parse_ysf,
			cls._parse_ysf_network_data,
		]
		for parser in parsers:
			instance = parser(logline)
			if instance:
				return instance
		raise ValueError(f'Log line does not match expected format: {logline}')

	@classmethod
	def _parse_dmr_voice(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.DMR_GW_PATTERN.match(logline) or cls.DMR_RF_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'DMR'
			obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.slot = int(match.group('slot'))
			obj.is_network = match.group('source') == 'network'
			obj.callsign = match.group('callsign').strip()
			obj.destination = match.group('destination').strip()
			obj.duration = float(match.group('duration'))
			obj.ber = float(match.group('ber'))
			obj._set_url(obj.callsign)
			if obj.is_network:
				obj.packet_loss = int(match.group('packet_loss'))
			else:
				obj.rssi3 = int(match.group('rssi3'))
			return obj
		return None

	# @classmethod
	# def _parse_dmr_data(cls, logline: str) -> Optional['MMDVMLogLine']:
	# 	match = cls.DMR_DATA_PATTERN.match(logline)
	# 	if match:
	# 		obj = cls()
	# 		obj.mode = 'DMR-D'
	# 		obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
	# 		obj.slot = int(match.group('slot'))
	# 		obj.is_network = match.group('source') == 'network'
	# 		obj.is_voice = False
	# 		obj.data_type = 'header'
	# 		obj.callsign = match.group('callsign').strip()
	# 		obj._set_url(obj.callsign)
	# 		obj.destination = match.group('destination').strip()
	# 		obj.block = int(match.group('block'))
	# 		return obj
	# 	return None

	@classmethod
	def _parse_dstar(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.DSTAR_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'D-Star'
			obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.is_network = match.group('source') == 'network'
			obj.callsign = remove_double_spaces(match.group('callsign').strip())
			obj.destination = match.group('destination').strip()
			obj.duration = float(match.group('duration'))
			obj.packet_loss = int(match.group('packet_loss'))
			obj.ber = float(match.group('ber'))
			obj._set_url(obj.callsign.split('/')[0].strip())
			return obj
		return None

	@classmethod
	def _parse_dstar_watchdog(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.DSTAR_WATCHDOG_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'D-Star'
			obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.is_network = match.group('source') == 'network'
			obj.duration = float(match.group('duration'))
			obj.packet_loss = int(match.group('packet_loss'))
			obj.ber = float(match.group('ber'))
			obj.is_watchdog = True
			return obj
		return None

	@classmethod
	def _parse_ysf(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.YSF_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'YSF'
			obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.is_network = match.group('source') == 'network'
			obj.is_voice = True
			obj.callsign = match.group('callsign').strip()
			obj.destination = f'DG-ID {match.group("dgid")}'
			obj.duration = float(match.group('duration'))
			obj.packet_loss = int(match.group('packet_loss'))
			obj.ber = float(match.group('ber'))
			obj._set_url(obj.callsign.split('-')[0].strip())
			return obj
		return None

	@classmethod
	def _parse_ysf_network_data(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.YSF_NETWORK_DATA_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'YSF-D'
			obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.is_network = match.group('source') == 'network'
			obj.is_voice = False
			obj.callsign = match.group('callsign').strip()
			obj.destination = f'DG-ID {match.group("dgid")} at {match.group("location").strip()}'
			obj._set_url(obj.callsign.split('-')[0].strip())
			return obj
		return None

	def _set_url(self, lookup_call: str):
		"""Sets the URL based on the callsign."""
		if lookup_call.isnumeric():
			self.url = f'https://database.radioid.net/database/view?id={lookup_call}'
		else:
			self.url = f'https://www.qrz.com/db/{lookup_call}'

	def __str__(self):
		"""Returns a string representation of the log line."""
		if self.rssi3 >= -93:
			self.rssi = '🟢S9'
		elif -99 <= self.rssi3 < -93:
			self.rssi = '🟢S8'
		elif -105 <= self.rssi3 < -99:
			self.rssi = '🟢S7'
		elif -111 <= self.rssi3 < -105:
			self.rssi = '🟠S6'
		elif -117 <= self.rssi3 < -111:
			self.rssi = '🟠S5'
		elif -123 <= self.rssi3 < -117:
			self.rssi = '🟠S4'
		elif -129 <= self.rssi3 < -123:
			self.rssi = '🟡S3'
		elif -135 <= self.rssi3 < -129:
			self.rssi = '🟡S2'
		elif -141 <= self.rssi3 < -135:
			self.rssi = '🔴S1'
		else:
			self.rssi = '🔴S0'
		self.rssi += f'+{93 + self.rssi3}dB ({self.rssi3}dBm)'
		self.is_kerchunk = True if self.duration < 2 else False
		base = f'Timestamp: {self.timestamp}, Mode: {self.mode}, Callsign: {self.callsign}, Destination: {self.destination}'
		if self.mode == 'DMR' or self.mode == 'DMR-D':
			base += f', Slot: {self.slot}'
			if self.is_voice:
				base += ', Type: Voice'
				if self.is_network:
					base += ', Source: Network'
					base += f', Duration: {self.duration}s, PL: {self.packet_loss}%, BER: {self.ber}%'
				else:
					base += ', Source: RF'
					base += f', Duration: {self.duration}s, BER: {self.ber}%, RSSI: {self.rssi}'
			else:
				base += ', Type: Data'
				if self.is_network:
					base += ', Source: Network'
				else:
					base += ', Source: RF'
				base += f', Blocks: {self.block}'
		return base

	def get_talkgroup_name(self) -> str:
		"""Returns the talkgroup name based on the destination."""
		tg_name = ''
		is_group = self.destination.startswith('TG ')
		tg_id_str = self.destination.split()[-1] if is_group else self.destination
		tg_map = get_talkgroup_ids()
		name = tg_map.get(tg_id_str)
		if not name and tg_id_str.isdigit():
			tg_id = int(tg_id_str)
			rules = get_dmrgateway_rules()
			required_type = 'TG' if is_group else 'PC'
			for rule in rules:
				if rule.get('type', 'TG') != required_type:
					continue
				if rule['slot'] != 0 and rule['slot'] != self.slot:
					continue
				if rule['start'] <= tg_id <= rule['end']:
					remapped_id = tg_id + rule['offset']
					name = f'{rule["name"]}: {remapped_id}'
					break
			if not name and len(tg_id_str) > 3:
				mcc = int(tg_id_str[:3])
				if mcc in MCC_CODES:
					_, code = MCC_CODES[mcc]
					name = f'{get_flag_emoji(code)} {code}'
		if name:
			tg_name = f' ({name})'
		return tg_name

	def get_caller_location(self) -> str:
		"""Returns the location of the caller based on the callsign."""
		caller = ''
		user_map = get_user_csv_data()
		user_info = user_map.get(self.callsign)
		if user_info:
			fname, country = user_info
			code = get_country_code(country)
			flag = get_flag_emoji(code)
			label = code if code else country
			caller = f' ({fname}) [{flag} {label}]'
		elif self.callsign.isdigit() and len(self.callsign) >= 3:
			mcc = int(self.callsign[:3])
			if mcc in MCC_CODES:
				_, code = MCC_CODES[mcc]
				caller = f' [{get_flag_emoji(code)} {code}]'
		return caller

	def get_telegram_message(self) -> str:
		"""Returns a formatted message for Telegram with emojis."""
		if self.mode == 'DMR':
			mode_icon = '📻'
		elif self.mode == 'DMR-D':
			mode_icon = '📟'
		elif self.mode == 'D-Star':
			mode_icon = '⭐'
		elif self.mode == 'YSF':
			mode_icon = '📡'
		elif self.mode == 'YSF-D':
			mode_icon = '📟'
		else:
			mode_icon = '📶'
		message = f'{mode_icon} Mode: <b>{self.mode}</b>'
		if self.mode == 'DMR' or self.mode == 'DMR-D':
			message += f' (Slot {self.slot})'
		time = (self.timestamp.replace(tzinfo=dt.timezone.utc) or dt.datetime.now()).astimezone().isoformat()
		message += f'\n🕒 Time: <b>{time}</b>'
		if self.url:
			message += f'\n📡 Caller: <b><a href="{self.url}">{self.callsign}</a>{self.get_caller_location()}</b>'
		else:
			message += f'\n📡 Caller: <b>{self.callsign}{self.get_caller_location()}</b>'
		message += f'\n🎯 Target: <b>{self.destination}{self.get_talkgroup_name()} [{"RF" if not self.is_network else "NET"}]</b>'
		if self.is_voice:
			message += '\n🗣️ Type: <b>Voice</b>'
			if self.is_kerchunk:
				message += ' (Kerchunk)'
			else:
				message += (
					f'\n⏰ Duration: <b>{humanize.precisedelta(dt.timedelta(seconds=self.duration), minimum_unit="seconds", format="%0.0f")}</b>'
				)
				if self.ber > 0:
					message += f'\n📊 BER: <b>{self.ber}%</b>'
				if self.is_network:
					if self.packet_loss > 0:
						message += f'\n📈 PL: <b>{self.packet_loss}%</b>'
				else:
					message += f'\n📶 RSSI: <b>{self.rssi}</b>'
		else:
			message += f'\n💾 Type: <b>Data {self.data_type.split()[-1].title()}</b>'
			if self.block > 0:
				message += f'\n📦 Blocks: <b>{self.block}</b>'
		if self.is_watchdog:
			message += '\n\n⚠️ Warning: <b>Network watchdog expired</b>'
		if self.mode == 'D-Star':
			if self.destination.startswith('CQCQCQ'):
				message += '\n\n📢 Action: <b>Call to all stations</b>'
			elif self.destination.endswith('L'):
				message += f'\n\n🔗 Action: <b>Link to {self.destination[:-1]}</b>'
			elif self.destination.endswith('U'):
				message += '\n\n❌ Action: <b>Unlink reflector</b>'
			elif self.destination.endswith('I'):
				message += '\n\nℹ️ Action: <b>Get repeater info</b>'
			elif self.destination.endswith('E'):
				message += '\n\n🔄 Action: <b>Echo test</b>'
		return message


async def logs_to_telegram(tg_message: str):
	"""Queues the log line to be sent to the Telegram bot."""
	if MESSAGE_QUEUE:
		await MESSAGE_QUEUE.put(tg_message)


async def telegram_message_worker(stop_event: asyncio.Event):
	"""Worker to process and send Telegram messages from the queue."""
	global TG_APP, MESSAGE_QUEUE
	logging.info('Starting Telegram message worker...')
	while not stop_event.is_set():
		try:
			if MESSAGE_QUEUE is None:
				await asyncio.sleep(1)
				continue
			try:
				tg_message = await asyncio.wait_for(MESSAGE_QUEUE.get(), timeout=1.0)
			except asyncio.TimeoutError:
				continue
			message = f'{tg_message}\n\n<code>{APP_NAME}</code>'
			if TG_APP:
				try:
					botmsg = await TG_APP.bot.send_message(
						chat_id=TG_CHATID,
						message_thread_id=TG_TOPICID,
						text=message,
						parse_mode='HTML',
						link_preview_options={'is_disabled': True, 'prefer_small_media': True, 'show_above_text': True},
					)
					logging.info('Sent message to Telegram: %s/%s/%s', botmsg.chat_id, botmsg.message_thread_id, botmsg.message_id)
				except Exception as e:
					logging.error('Failed to send message to Telegram: %s', e)
			MESSAGE_QUEUE.task_done()
			await asyncio.sleep(0.5)
		except Exception as e:
			logging.error('Error in Telegram message worker: %s', e)


async def mmdvm_logs_observer(stop_event: asyncio.Event):
	"""Watches the MMDVM logs and sends updates to the Telegram bot."""
	global TG_APP
	logging.info('Starting MMDVM log file retrieval...')
	last_event: Optional[datetime] = None
	current_log_path: Optional[str] = None
	while not stop_event.is_set():
		try:
			latest_log = get_latest_mmdvm_log_path()
			if current_log_path != latest_log:
				logging.info('Switching to new log file: %s', latest_log)
				if latest_log:
					if current_log_path:
						await logs_to_telegram(
							f'📃 {APP_NAME.split("-")[0]} Log Changed\nOld File: <s>{os.path.basename(current_log_path)}</s>\nNew File: <b>{os.path.basename(latest_log)}</b>'
						)
					else:
						await logs_to_telegram(f'📃 {APP_NAME.split("-")[0]} Monitoring Log\nFile: <b>{os.path.basename(latest_log)}</b>')
				current_log_path = latest_log
			if current_log_path:
				last_line = get_last_line_of_file(current_log_path)
				logging.debug('Last line of log file: %s', last_line)
				if any(pattern in last_line for pattern in RELEVANT_LOG_PATTERNS):
					parsed_line = MMDVMLogLine.from_logline(last_line)
					logging.debug('Parsed log line: %s', parsed_line)
					if parsed_line.timestamp and (last_event is None or parsed_line.timestamp > last_event):
						logging.info('New log entry: %s', parsed_line)
						last_event = parsed_line.timestamp
						if not (GW_IGNORE_TIME_MESSAGES and '/TIME' in parsed_line.callsign):
							tg_message = parsed_line.get_telegram_message()
							if tg_message and TG_APP:
								await logs_to_telegram(tg_message)
						elif GW_IGNORE_TIME_MESSAGES:
							logging.info('Ignoring time message from gateway.')
					else:
						logging.debug('No new log entry found.')
				else:
					logging.debug('Line does not contain transmission end marker, skipping.')
			else:
				logging.error('No log file path available')
		except ValueError as e:
			logging.debug('Could not parse log line: %s', e)
		except OSError as e:
			logging.error('File system error reading log file: %s', e)
		except Exception as e:
			logging.error('Error in observer loop: %s', e)
		try:
			await asyncio.wait_for(stop_event.wait(), timeout=1.0)
		except asyncio.TimeoutError:
			pass


async def main():
	"""Main function to initialize and run the Telegram bot and logs observer."""
	global TG_APP, MESSAGE_QUEUE
	load_env_variables()
	MESSAGE_QUEUE = asyncio.Queue()
	stop_event = asyncio.Event()
	loop = asyncio.get_running_loop()
	for sig in (signal.SIGINT, signal.SIGTERM):
		loop.add_signal_handler(sig, lambda: stop_event.set())
	worker_task = asyncio.create_task(telegram_message_worker(stop_event))
	tg_app_built = False
	try:
		while not tg_app_built and not stop_event.is_set():
			try:
				TG_APP = ApplicationBuilder().token(TG_BOTTOKEN).build()
				tg_app_built = True
				logging.info('Telegram application built successfully.')
			except Exception as e:
				logging.error('Error building Telegram application: %s', e)
				try:
					await asyncio.wait_for(stop_event.wait(), timeout=5)
				except asyncio.TimeoutError:
					pass
		if tg_app_built:
			assert TG_APP is not None
			async with TG_APP:
				tg_app_started = False
				while not tg_app_started and not stop_event.is_set():
					try:
						logging.info('Starting Telegram bot...')
						await TG_APP.initialize()
						await TG_APP.start()
						tg_app_started = True
						logging.info('Telegram bot started successfully.')
					except Exception as e:
						logging.error('Error starting Telegram bot: %s', e)
						try:
							await asyncio.wait_for(stop_event.wait(), timeout=5)
						except asyncio.TimeoutError:
							pass
				if tg_app_started:
					try:
						logging.info('Starting MMDVM logs observer...')
						await logs_to_telegram(f'🚀 {APP_NAME.split("-")[0]} Started')
						await mmdvm_logs_observer(stop_event)
					except asyncio.CancelledError:
						logging.info('MMDVM logs observer cancelled.')
					finally:
						try:
							await logs_to_telegram(f'🛑 {APP_NAME.split("-")[0]} Stopping')
						except Exception as e:
							logging.error('Failed to send stop message: %s', e)
						await TG_APP.stop()
	finally:
		if not stop_event.is_set():
			stop_event.set()
		await worker_task


if __name__ == '__main__':
	configure_logging()
	try:
		logging.info('Starting the application...')
		asyncio.run(main())
	except KeyboardInterrupt:
		logging.info('Stopping application...')
	except Exception as e:
		logging.error('An error occurred: %s', e)
	finally:
		logging.info('Exiting script...')
