#!/usr/bin/python3
"""MMDVM LastHeard - Telegram bot to monitor the last transmissions of a MMDVM gateway"""

import asyncio
import configparser
import datetime as dt
import difflib
import glob
import logging
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
from country_codes import COUNTRY_CODES
from dotenv import load_dotenv
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
	'received RF data header',
	'received network data header',
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
	meta = {'name': 'MMDVM_LastHeard', 'version': '0.0.0', 'github': 'https://github.com/HafiziRuslan/MMDVM-Last-Heard'}
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
	logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%dT%H:%M:%S', format='%(asctime)s | %(levelname)s | %(message)s')


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


@lru_cache(maxsize=1)
def get_talkgroup_ids() -> dict:
	"""Reads and caches the talkgroup list from files"""
	tg_map = {}
	file_configs = [
		('/usr/local/etc/TGList_TGIF.txt', ';', 0, 1),
		('/usr/local/etc/TGList_FreeStarIPSC.txt', ',', 0, 1),
		('/usr/local/etc/TGList_SystemX.txt', ',', 0, 1),
		('/usr/local/etc/TGList_FreeDMR.txt', ',', 0, 1),
		('/usr/local/etc/TGList_ADN.txt', ',', 0, 1),
		('/usr/local/etc/TGList_DMRp.txt', ',', 0, 1),
		('/usr/local/etc/TGList_QuadNet.txt', ',', 0, 1),
		('/usr/local/etc/TGList_AmComm.txt', ',', 0, 1),
		('/usr/local/etc/YSFHosts.txt', ';', 0, 1),
		('/usr/local/etc/TGList_NXDN.txt', ';', 0, 1),
		('/usr/local/etc/TGList_P25.txt', ';', 0, 1),
		('/usr/local/etc/TGList_BM.txt', ';', 0, 2),
		('/usr/local/etc/groups.txt', ' ', 0, 1),
	]
	processed_files = set()
	for pattern, delimiter, id_idx, name_idx in file_configs:
		files = glob.glob(pattern)
		for tg_file in files:
			processed_files.add(tg_file)
			filename = os.path.basename(tg_file)
			name_part = os.path.splitext(filename)[0]
			suffix = name_part[7:] if name_part.startswith('TGList_') else name_part
			read_talkgroup_file(tg_file, delimiter, id_idx, name_idx, tg_map, suffix=suffix, overwrite=True)
	for tg_file in glob.glob('/usr/local/etc/TGList_*.txt'):
		if tg_file not in processed_files:
			filename = os.path.basename(tg_file)
			name_part = os.path.splitext(filename)[0]
			suffix = name_part[7:] if name_part.startswith('TGList_') else name_part
			read_talkgroup_file(tg_file, ';', 0, 1, tg_map, suffix=suffix, overwrite=False)
	return tg_map


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
	DMR_GW_PATTERN = re.compile(
		r'^M: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) '
		r'DMR Slot (?P<slot>\d), received (?P<source>network) (?:late entry|voice header|end of voice transmission) '
		r'from (?P<callsign>[\w\d]+) to (?P<destination>(TG [\d\w]+)|[\d\w]+)'
		r'(?:, (?P<duration>[\d\.]+) seconds, (?P<packet_loss>[\d\.]+)% packet loss, BER: (?P<ber>[\d\.]+)%)'
	)
	DMR_RF_PATTERN = re.compile(
		r'^M: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) '
		r'DMR Slot (?P<slot>\d), received (?P<source>RF) (?:late entry|voice header|end of voice transmission) '
		r'from (?P<callsign>[\w\d]+) to (?P<destination>(TG [\d\w]+)|[\d\w]+)'
		r'(?:, (?P<duration>[\d\.]+) seconds, BER: (?P<ber>[\d\.]+)%, RSSI: (?P<rssi1>-[\d]+)/(?P<rssi2>-[\d]+)/(?P<rssi3>-[\d]+) dBm)'
	)
	DMR_DATA_PATTERN = re.compile(
		r'^M: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) '
		r'DMR Slot (?P<slot>\d), received (?P<source>network|RF) data header '
		r'from (?P<callsign>[\w\d]+) to (?P<destination>(TG [\d\w]+)|[\d\w]+)'
		r'(?:, (?P<block>[\d]+) blocks)'
	)
	DSTAR_PATTERN = re.compile(
		r'^M: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) '
		r'D-Star, (?:received )?(?P<source>network|RF) end of transmission '
		r'from (?P<callsign>[\w\d\s/]+) to (?P<destination>[\w\d\s]+)'
		r'(?:, | , )(?P<duration>[\d\.]+) seconds,\s+(?P<packet_loss>[\d\.]+)% packet loss, BER: (?P<ber>[\d\.]+)%'
	)
	DSTAR_WATCHDOG_PATTERN = re.compile(
		r'^M: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) '
		r'D-Star, (?P<source>network|RF) watchdog has expired'
		r', (?P<duration>[\d\.]+) seconds,\s+(?P<packet_loss>[\d\.]+)% packet loss, BER: (?P<ber>[\d\.]+)%'
	)
	YSF_PATTERN = re.compile(
		r'^M: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) '
		r'YSF, received (?P<source>network|RF) end of transmission '
		r'from (?P<callsign>[\w\d\-/]+) to DG-ID (?P<dgid>\d+)'
		r', (?P<duration>[\d\.]+) seconds, (?P<packet_loss>[\d\.]+)% packet loss, BER: (?P<ber>[\d\.]+)%'
	)
	YSF_NETWORK_DATA_PATTERN = re.compile(
		r'^M: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) '
		r'YSF, received network data '
		r'from (?P<callsign>[\w\d\-/]+)\s+to DG-ID (?P<dgid>\d+) at (?P<location>\S+)'
	)

	@classmethod
	def from_logline(cls, logline: str) -> 'MMDVMLogLine':
		"""Factory method to create an MMDVMLogLine instance from a log line."""
		parsers = [
			cls._parse_dmr_gw,
			cls._parse_dmr_rf,
			cls._parse_dmr_data,
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
	def _parse_dmr_gw(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.DMR_GW_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'DMR'
			obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.slot = int(match.group('slot'))
			obj.is_network = match.group('source') == 'network'
			obj.callsign = match.group('callsign').strip()
			obj.destination = match.group('destination').strip()
			obj.duration = float(match.group('duration'))
			obj.packet_loss = int(match.group('packet_loss'))
			obj.ber = float(match.group('ber'))
			obj._set_url(obj.callsign)
			return obj
		return None

	@classmethod
	def _parse_dmr_rf(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.DMR_RF_PATTERN.match(logline)
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
			obj.rssi3 = int(match.group('rssi3'))
			obj._set_url(obj.callsign)
			return obj
		return None

	@classmethod
	def _parse_dmr_data(cls, logline: str) -> Optional['MMDVMLogLine']:
		match = cls.DMR_DATA_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'DMR-D'
			obj.timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.slot = int(match.group('slot'))
			obj.is_network = match.group('source') == 'network'
			obj.is_voice = False
			obj.callsign = match.group('callsign').strip()
			obj.destination = match.group('destination').strip()
			obj.block = int(match.group('block'))
			obj._set_url(obj.callsign)
			return obj
		return None

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
			self.rssi = '🟩S9'
		elif -99 <= self.rssi3 < -93:
			self.rssi = '🟩S8'
		elif -105 <= self.rssi3 < -99:
			self.rssi = '🟩S7'
		elif -111 <= self.rssi3 < -105:
			self.rssi = '🟨S6'
		elif -117 <= self.rssi3 < -111:
			self.rssi = '🟨S5'
		elif -123 <= self.rssi3 < -117:
			self.rssi = '🟨S4'
		elif -129 <= self.rssi3 < -123:
			self.rssi = '🟨S3'
		elif -135 <= self.rssi3 < -129:
			self.rssi = '🟥S2'
		elif -141 <= self.rssi3 < -135:
			self.rssi = '🟥S1'
		else:
			self.rssi = '🟥S0'
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
		if self.destination.startswith('TG '):
			tg_id = self.destination.split()[-1]
			tg_map = get_talkgroup_ids()
			name = tg_map.get(tg_id)
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
			if code:
				flag = ''.join(chr(ord(c) + 127397) for c in code.upper())
				caller = f' ({fname}) [{flag} {code}]'
			else:
				caller = f' ({fname}) [{country}]'
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
		message = f'{mode_icon} <b>Mode</b>: {self.mode}'
		if self.mode == 'DMR' or self.mode == 'DMR-D':
			message += f' (Slot {self.slot})'
		message += f'\n🕒 <b>Time</b>: {datetime.strftime(self.timestamp.replace(tzinfo=dt.timezone.utc), "%d-%b-%Y %H:%M:%S %Z") if self.timestamp else dt.datetime.now(dt.timezone.utc).strftime("%d-%b-%Y %H:%M:%S %Z")}'
		if self.url:
			message += f'\n📡 <b>Caller</b>: <a href="{self.url}">{self.callsign}</a>{self.get_caller_location()}'
		else:
			message += f'\n📡 <b>Caller</b>: {self.callsign}{self.get_caller_location()}'
		message += f'\n🎯 <b>Target</b>: {self.destination}{self.get_talkgroup_name()}'
		message += f' [{"RF" if not self.is_network else "NET"}]'
		if self.is_voice:
			message += '\n🗣️ <b>Type</b>: Voice'
			if self.is_kerchunk:
				message += ' (Kerchunk)'
			else:
				message += (
					f'\n⏰ <b>Duration</b>: {humanize.precisedelta(dt.timedelta(seconds=self.duration), minimum_unit="seconds", format="%0.0f")}'
				)
				if self.ber > 0:
					message += f'\n📊 <b>BER</b>: {self.ber}%'
				if self.is_network:
					if self.packet_loss > 0:
						message += f'\n📈 <b>PL</b>: {self.packet_loss}%'
				else:
					message += f'\n📶 <b>RSSI</b>: {self.rssi}'
		else:
			message += '\n💾 <b>Type</b>: Data'
			message += f'\n📦 <b>Blocks</b>: {self.block}'
		if self.is_watchdog:
			message += '\n\n⚠️ <b>Warning</b>: Network watchdog expired'
		if self.mode == 'D-Star':
			if self.destination.startswith('CQCQCQ'):
				message += '\n\n📢 <b>Action</b>: Call to all stations'
			elif self.destination.endswith('L'):
				message += f'\n\n🔗 <b>Action</b>: Link to {self.destination[:-1]}'
			elif self.destination.endswith('U'):
				message += '\n\n❌ <b>Action</b>: Unlink reflector'
			elif self.destination.endswith('I'):
				message += '\n\nℹ️ <b>Action</b>: Get repeater info'
			elif self.destination.endswith('E'):
				message += '\n\n🔄 <b>Action</b>: Echo test'
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
			message = f'{tg_message}\n\n{APP_NAME}'
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
						await logs_to_telegram(f'📂 <b>Log File Changed</b>\nNew file: {os.path.basename(latest_log)}')
					else:
						await logs_to_telegram(f'📂 <b>Log File Monitoring Started</b>\nFile: {os.path.basename(latest_log)}')
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
						await logs_to_telegram(f'🚀 <b>{APP_NAME} Started</b>')
						await mmdvm_logs_observer(stop_event)
					except asyncio.CancelledError:
						logging.info('MMDVM logs observer cancelled.')
					finally:
						try:
							if TG_APP:
								await TG_APP.bot.send_message(
									chat_id=TG_CHATID, message_thread_id=TG_TOPICID, text=f'🛑 <b>{APP_NAME} Stopping...</b>', parse_mode='HTML'
								)
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
