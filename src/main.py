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
from dataclasses import field
from functools import lru_cache
from typing import Optional

import humanize
from dotenv import load_dotenv
from codes import COUNTRY_CODES, MCC_CODES
from telegram.ext import Application as TelegramApplication
from telegram.ext import ApplicationBuilder


@lru_cache
def _get_app_metadata():
	repo_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
	git_sha = ''
	if shutil.which('git'):
		try:
			git_sha = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD^'], cwd=repo_path).decode('ascii').strip()
		except Exception:
			pass
	meta = {'name': 'MMDVM_LastHeard', 'version': '0.1', 'github': 'https://git.new/mmdvmlhbot'}
	try:
		with open(os.path.join(repo_path, 'pyproject.toml'), 'rb') as f:
			data = tomllib.load(f).get('project', {})
			meta.update({k: data.get(k, meta[k]) for k in ['name', 'version']})
			meta['github'] = data.get('urls', {}).get('github', meta['github'])
	except Exception as e:
		logging.warning('Failed to load project metadata: %s', e)
	return f'{"-".join(filter(None, [meta["name"], meta["version"], git_sha]))}', meta['github']


class ConfigManager:
	"""Manages all application configuration."""

	def __init__(self):
		load_dotenv()
		self.tg_bot_token = os.getenv('TG_BOTTOKEN', '')
		self.tg_chat_id = os.getenv('TG_CHATID', '')
		self.tg_topic_id = os.getenv('TG_TOPICID', '0')
		self.gw_ignore_time_messages = os.getenv('GW_IGNORE_MESSAGES', 'True').lower() == 'true'

		# Map numeric LOG_LEVEL from environment variable
		# [0: off, 1: debug, 2: info, 3: warning, 4: error, 5: critical]
		log_level_map = {
			0: logging.CRITICAL + 1,  # Effectively 'off' by setting level higher than CRITICAL
			1: logging.DEBUG,
			2: logging.INFO,
			3: logging.WARNING,
			4: logging.ERROR,
			5: logging.CRITICAL,
		}

		log_level_raw = os.getenv('LOG_LEVEL')
		try:
			log_level_int = int(log_level_raw)
			self.log_level = log_level_map.get(log_level_int, logging.INFO)
		except (TypeError, ValueError):
			# Fallback to INFO if LOG_LEVEL is not set or not a valid integer
			logging.warning('LOG_LEVEL environment variable must be an integer between 0 and 5. Defaulting to INFO.')
			self.log_level = logging.INFO

		# Parse LOG_MAX_SIZE and LOG_MAX_COUNT as integers, providing string defaults
		self.log_max_size_mb = float(os.getenv('LOG_MAX_SIZE', '1'))  # Default 1MB
		self.log_max_count = int(os.getenv('LOG_MAX_COUNT', '3'))  # Default 3 backups

		self.app_name, self.project_url = _get_app_metadata()
		self.app_name_short = self.app_name.split('-')[0]

		self.relevant_log_patterns = ['end of voice transmission', 'end of transmission', 'watchdog has expired']

		if not self.tg_bot_token or not self.tg_chat_id:
			logging.warning('TG_BOTTOKEN or TG_CHATID is not set in the environment variables.')
		if self.gw_ignore_time_messages:
			logging.info('GW_IGNORE_MESSAGES is set to true, messages from the gateway will be ignored.')


class LoggingManager:
	"""Manages the application's logging configuration."""

	class ISO8601Formatter(logging.Formatter):
		"""A logging formatter that uses ISO 8601 format for timestamps."""

		def formatTime(self, record, datefmt=None):
			return dt.datetime.fromtimestamp(record.created, dt.timezone.utc).astimezone().isoformat(timespec='milliseconds')

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
		"""A filter that allows log records of a specific level."""

		def __init__(self, level):
			self.level = level

		def filter(self, record):
			return record.levelno == self.level

	def __init__(
		self,
		log_dir: str = '/var/log/mmdvmlhbot',
		fallback_log_dir: str = 'logs',
		log_level: int = logging.INFO,
		log_max_size_mb: int = 1,
		log_max_count: int = 3,
	):
		self.log_dir = log_dir
		if not os.path.exists(self.log_dir) or not os.access(self.log_dir, os.W_OK):
			self.log_dir = fallback_log_dir
		if not os.path.exists(self.log_dir):
			os.makedirs(self.log_dir)
		self.log_level = log_level
		self.log_max_size_bytes = log_max_size_mb * 1024 * 1024
		self.log_max_count = log_max_count
		self._formatter = self.ISO8601Formatter('%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(name)s.%(funcName)s:%(lineno)d | %(message)s')

	def setup(self):
		"""Sets up the logging configuration."""
		self._set_library_log_levels()
		logger = logging.getLogger()
		logger.setLevel(self.log_level)
		self._configure_console_handler(logger)
		self._configure_file_handlers(logger)

	def _set_library_log_levels(self):
		"""Sets specific log levels for external libraries."""
		external_libs = ['asyncio', 'hpack', 'httpx', 'telegram', 'urllib3']
		for lib in external_libs:
			logging.getLogger(lib).setLevel(self.log_level)

	def _configure_console_handler(self, logger: logging.Logger):
		"""Configures and adds the console log handler."""
		console_handler = logging.StreamHandler()
		console_handler.setLevel(logging.WARNING)  # Console handler still shows WARNING by default
		console_handler.setFormatter(self._formatter)
		logger.addHandler(console_handler)

	def _configure_file_handlers(self, logger: logging.Logger):
		"""Configures and adds rotating file handlers for different log levels."""
		levels_map = {
			logging.DEBUG: '1-debug.log',
			logging.INFO: '2-info.log',
			logging.WARNING: '3-warning.log',
			logging.ERROR: '4-error.log',
			logging.CRITICAL: '5-critical.log',
		}
		for level, filename in levels_map.items():
			# Only create handlers for levels at or above the configured log_level
			if level >= self.log_level:
				try:
					handler = self.NumberedRotatingFileHandler(
						os.path.join(self.log_dir, filename), maxBytes=self.log_max_size_bytes, backupCount=self.log_max_count
					)
					handler.setLevel(level)
					handler.addFilter(self.LevelFilter(level))
					handler.setFormatter(self._formatter)
					logger.addHandler(handler)
				except (OSError, PermissionError) as e:
					logging.error('Failed to create %s: %s', filename, e)


class Formatter:
	"""A collection of formatting utility functions."""

	@staticmethod
	def remove_double_spaces(text: str) -> str:
		"""Removes double spaces from a string."""
		while '  ' in text:
			text = text.replace('  ', ' ')
		return text

	@staticmethod
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

	@staticmethod
	def get_flag_emoji(country_code: str) -> str:
		"""Converts a two-letter country code to a flag emoji."""
		if country_code and len(country_code) == 2:
			return ''.join(chr(ord(c) + 127397) for c in country_code.upper())
		return '🌐'


class DMRGatewayManager:
	"""Manages loading and caching of DMRGateway configuration."""

	def __init__(self, config_files: list[str] = None):
		self._cache = {'path': None, 'mtime': 0, 'rules': [], 'networks': []}
		self._conf_files = config_files or ['/etc/dmrgateway', '/etc/DMRGateway.ini', '/opt/DMRGateway/DMRGateway.ini']

	def get_rules(self) -> list:
		"""Returns the list of rewrite rules."""
		self._update_cache()
		return self._cache['rules']

	def get_networks(self) -> list:
		"""Returns the list of configured networks."""
		self._update_cache()
		return self._cache['networks']

	def _update_cache(self):
		"""Updates the cache if the configuration file has changed."""
		config_path = self._cache['path']
		if not config_path or not os.path.isfile(config_path):
			config_path = None
			for f in self._conf_files:
				if os.path.isfile(f):
					config_path = f
					break
		if config_path:
			try:
				mtime = os.path.getmtime(config_path)
				if config_path == self._cache['path'] and mtime == self._cache['mtime']:
					return
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
				self._cache = {'path': config_path, 'mtime': mtime, 'rules': rules, 'networks': networks}
			except Exception as e:
				logging.error('Error reading DMRGateway config %s: %s', config_path, e)


class TalkgroupManager:
	"""Manages loading, caching, and retrieving talkgroup information."""

	def __init__(self, dmr_gateway_manager: DMRGatewayManager):
		"""Initializes the TalkgroupManager."""
		self._dmr_gateway_manager = dmr_gateway_manager
		self._cache = {'mtimes': {}, 'tg_map': {}}

	def get_map(self) -> dict:
		"""Reads and caches the talkgroup list from files, reloading if files change."""
		current_mtimes, expanded_configs, catch_all_files = self._collect_files_and_mtimes()
		if current_mtimes == self._cache.get('mtimes') and self._cache.get('tg_map'):
			return self._cache['tg_map']
		tg_map = {}
		processed_files = set()
		for files, delimiter, id_idx, name_idx in expanded_configs:
			for tg_file in files:
				processed_files.add(tg_file)
				filename = os.path.basename(tg_file)
				name_part = os.path.splitext(filename)[0]
				suffix = name_part[7:] if name_part.startswith('TGList_') else name_part
				self._read_talkgroup_file(tg_file, delimiter, id_idx, name_idx, tg_map, suffix=suffix, overwrite=True)
		for tg_file in catch_all_files:
			if tg_file not in processed_files:
				filename = os.path.basename(tg_file)
				name_part = os.path.splitext(filename)[0]
				suffix = name_part[7:] if name_part.startswith('TGList_') else name_part
				self._read_talkgroup_file(tg_file, ';', 0, 1, tg_map, suffix=suffix, overwrite=False)
		self._apply_special_rules(tg_map)
		self._cache = {'mtimes': current_mtimes, 'tg_map': tg_map}
		return tg_map

	def _read_talkgroup_file(
		self, file_path: str, delimiter: str, id_idx: int, name_idx: int, tg_map: dict, suffix: str = '', overwrite: bool = True
	):
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

	def _get_static_sources(self) -> list[tuple[str, str, int, int]]:
		"""Returns the list of static talkgroup file sources."""
		return [
			('/usr/local/etc/groups.txt', ':', 0, 1),
			('/usr/local/etc/groupsNextion.txt', ',', 0, 1),
			('/usr/local/etc/TGList_ADN', ',', 0, 1),
			('/usr/local/etc/TGList_ADN-NoPrefix', ',', 0, 1),
			('/usr/local/etc/TGList_BM', ';', 0, 2),
			('/usr/local/etc/TGList_DMRp', ',', 0, 1),
			('/usr/local/etc/TGList_DMRp_NoPrefix', ',', 0, 1),
			('/usr/local/etc/TGList_FreeDMR', ',', 0, 1),
			('/usr/local/etc/TGList_FreeStarIPSC', ',', 0, 1),
			('/usr/local/etc/TGList_NXDN', ';', 0, 2),
			('/usr/local/etc/TGList_P25', ';', 0, 2),
			('/usr/local/etc/TGList_QuadNet', ',', 0, 1),
			('/usr/local/etc/TGList_QuadNet-NoPrefix', ',', 0, 1),
			('/usr/local/etc/TGList_SystemX', ',', 0, 1),
			('/usr/local/etc/TGList_TGIF', ';', 0, 1),
			('/usr/local/etc/TGList_YSF', ';', 0, 1),
		]

	def _get_dynamic_sources(self) -> list[tuple[str, str, int, int]]:
		"""Returns the list of dynamic talkgroup file sources based on DMRGateway config."""
		configs = []
		for net in self._dmr_gateway_manager.get_networks():
			name_clean = net.split('_')[0]
			fpath = f'/usr/local/etc/TGList_{name_clean}.txt'
			name_idx = 2 if 'BM' in name_clean else 1
			configs.append((fpath, ';', 0, name_idx))
		return configs

	def _collect_files_and_mtimes(self) -> tuple[dict, list, list]:
		"""Collects all talkgroup files and their modification times."""
		mtimes = {}
		expanded_configs = []
		sources = self._get_static_sources() + self._get_dynamic_sources()
		for pattern, delimiter, id_idx, name_idx in sources:
			files = glob.glob(pattern)
			expanded_configs.append((files, delimiter, id_idx, name_idx))
			for f in files:
				try:
					mtimes[f] = os.path.getmtime(f)
				except OSError:
					pass
		catch_all_files = glob.glob('/usr/local/etc/TGList_*.txt')
		for f in catch_all_files:
			try:
				mtimes[f] = os.path.getmtime(f)
			except OSError:
				pass
		return mtimes, expanded_configs, catch_all_files

	def _apply_special_rules(self, tg_map: dict):
		"""Applies special talkgroup rules for DMRGateway and MCCs."""
		for rule in self._dmr_gateway_manager.get_rules():
			if rule.get('type') in ('TG', 'PC'):
				for target_tg, label in [(4000, 'Disconnect'), (9990, 'Parrot'), (31000, 'Parrot')]:
					src_tg = target_tg - rule['offset']
					if rule['start'] <= src_tg <= rule['end']:
						tg_map[str(src_tg)] = label
		for mcc, (country, _) in MCC_CODES.items():
			tg_map[f'{mcc}990'] = f'{country} Text Message'
			tg_map[f'{mcc}997'] = f'{country} Parrot'
			tg_map[f'{mcc}999'] = f'{country} ARS/RRS/GPS'


class UserManager:
	"""Manages loading and caching of user data from user.csv and DMRIds.dat."""

	def __init__(self, user_csv_path='/usr/local/etc/user.csv', dmr_ids_path='/usr/local/etc/DMRIds.dat'):
		"""Initializes the UserManager."""
		self._user_csv_path = user_csv_path or '.sample/user.csv'
		self._dmr_ids_path = dmr_ids_path or '.sample/DMRids.dat'
		self._cache = {'mtime_csv': 0, 'mtime_dat': 0, 'user_map': {}}

	def get_map(self) -> dict:
		"""Returns the user map, reloading from file if it has changed."""
		try:
			mtime_csv = os.path.getmtime(self._user_csv_path)
		except OSError:
			mtime_csv = 0
		try:
			mtime_dat = os.path.getmtime(self._dmr_ids_path)
		except OSError:
			mtime_dat = 0

		if mtime_csv == self._cache.get('mtime_csv') and mtime_dat == self._cache.get('mtime_dat') and self._cache.get('user_map'):
			return self._cache['user_map']

		user_map = self._load_data()
		self._cache = {'mtime_csv': mtime_csv, 'mtime_dat': mtime_dat, 'user_map': user_map}
		return user_map

	def _load_data(self) -> dict:
		"""Loads user data, preferring user.csv and falling back to DMRIds.dat."""
		user_map = self._load_from_user_csv()
		if not user_map:
			logging.warning('Could not load user data from %s, falling back to %s.', self._user_csv_path, self._dmr_ids_path)
			user_map = self._load_from_dmr_ids()
		return user_map

	def _load_from_user_csv(self) -> dict:
		"""Loads user data from the user.csv file."""
		if not os.path.isfile(self._user_csv_path):
			return {}
		encodings = ['utf-8', 'latin-1']
		for encoding in encodings:
			try:
				user_map = {}
				with open(self._user_csv_path, 'r', encoding=encoding, errors='replace') as file:
					for line in file:
						parts = line.strip().split(',')
						if len(parts) >= 3:
							ccs7 = parts[0].strip()
							call = parts[1].strip()
							fname = parts[2].strip()
							country = parts[-1].strip()
							if call:
								user_map[call] = (ccs7, fname, country)
								user_map[ccs7] = (call, fname, country)
				logging.debug('Successfully loaded user data from %s with %s encoding.', self._user_csv_path, encoding)
				return user_map
			except UnicodeDecodeError:
				logging.warning('UnicodeDecodeError with %s for %s. Trying next.', self._user_csv_path)
			except Exception as e:
				logging.error('Error reading user file %s: %s', self._user_csv_path, e)
				break
		return {}

	def _load_from_dmr_ids(self) -> dict:
		"""Loads user data from the DMRIds.dat file."""
		if not os.path.isfile(self._dmr_ids_path):
			return {}
		encodings = ['utf-8', 'latin-1']
		for encoding in encodings:
			try:
				user_map = {}
				with open(self._dmr_ids_path, 'r', encoding=encoding, errors='replace') as file:
					for line in file:
						line = line.strip()
						if not line or line.startswith('#'):
							continue
						parts = line.split('\t')
						if len(parts) >= 3:
							ccs7 = parts[0].strip()
							call = parts[1].strip()
							fname = parts[2].strip()
							country = ''
							if ccs7.isdigit() and len(ccs7) >= 3:
								mcc = int(ccs7[:3])
								if mcc in MCC_CODES:
									country, _ = MCC_CODES[mcc]
							if call:
								user_map[call] = (ccs7, fname, country)
								user_map[ccs7] = (call, fname, country)
				logging.debug('Successfully loaded user data from %s with %s encoding.', self._dmr_ids_path, encoding)
				return user_map
			except UnicodeDecodeError:
				logging.warning('UnicodeDecodeError with %s for %s. Trying next.', encoding, self._dmr_ids_path)
			except Exception as e:
				logging.error('Error reading user file %s: %s', self._dmr_ids_path, e)
				break
		return {}

class LogFileReader:
	"""Handles finding and reading MMDVM log files."""

	def __init__(self):
		self.log_dir = self._find_log_dir()

	def _find_log_dir(self) -> str:
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

	def get_latest_log_path(self) -> Optional[str]:
		"""Finds and returns the path to the most recent MMDVM log file."""
		log_files = glob.glob(os.path.join(self.log_dir, 'MMDVM-*.log'))
		if not log_files:
			return None
		log_files.sort(key=os.path.getmtime, reverse=True)
		latest_log = log_files[0]
		# logging.debug('Latest MMDVM log file: %s', latest_log)
		return latest_log

	@staticmethod
	def get_last_line(file_path: str) -> str:
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


class DataManager:
	"""Central manager for all data sources."""

	def __init__(self):
		self.dmr_gateway = DMRGatewayManager()
		self.talkgroups = TalkgroupManager(self.dmr_gateway)
		self.users = UserManager()
		self.log_reader = LogFileReader()


@dataclass
class MMDVMLogLine:
	timestamp: Optional[dt.datetime] = None
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
	data_manager: 'DataManager' = field(init=False, repr=False)
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
	def from_logline(cls, logline: str, data_manager: 'DataManager') -> 'MMDVMLogLine':
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
				instance.data_manager = data_manager
				return instance
		raise ValueError(f'Log line does not match expected format: {logline}')

	@classmethod
	def _parse_dmr_voice(cls, logline: str) -> Optional['MMDVMLogLine']:
		"""Parses a DMR voice transmission log line."""
		match = cls.DMR_GW_PATTERN.match(logline) or cls.DMR_RF_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'DMR'
			obj.timestamp = dt.datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
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
				obj._format_rssi_string()
			return obj
		return None

	# @classmethod
	# def _parse_dmr_data(cls, logline: str) -> Optional['MMDVMLogLine']:
	# 	"""Parses a DMR data transmission log line."""
	# 	match = cls.DMR_DATA_PATTERN.match(logline)
	# 	if match:
	# 		obj = cls()
	# 		obj.mode = 'DMR-D'
	# 		obj.timestamp = dt.datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
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
		"""Parses a D-Star transmission log line."""
		match = cls.DSTAR_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'D-Star'
			obj.timestamp = dt.datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.is_network = match.group('source') == 'network'
			obj.callsign = Formatter.remove_double_spaces(match.group('callsign').strip())
			obj.destination = match.group('destination').strip()
			obj.duration = float(match.group('duration'))
			obj.packet_loss = int(match.group('packet_loss'))
			obj.ber = float(match.group('ber'))
			obj._set_url(obj.callsign.split('/')[0].strip())
			return obj
		return None

	@classmethod
	def _parse_dstar_watchdog(cls, logline: str) -> Optional['MMDVMLogLine']:
		"""Parses a D-Star watchdog log line."""
		match = cls.DSTAR_WATCHDOG_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'D-Star'
			obj.timestamp = dt.datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
			obj.is_network = match.group('source') == 'network'
			obj.duration = float(match.group('duration'))
			obj.packet_loss = int(match.group('packet_loss'))
			obj.ber = float(match.group('ber'))
			obj.is_watchdog = True
			return obj
		return None

	@classmethod
	def _parse_ysf(cls, logline: str) -> Optional['MMDVMLogLine']:
		"""Parses a YSF transmission log line."""
		match = cls.YSF_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'YSF'
			obj.timestamp = dt.datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
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
		"""Parses a YSF network data transmission log line."""
		match = cls.YSF_NETWORK_DATA_PATTERN.match(logline)
		if match:
			obj = cls()
			obj.mode = 'YSF-D'
			obj.timestamp = dt.datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
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

	def _format_rssi_string(self):
		"""Formats the RSSI string."""
		if self.rssi3 >= -93:
			s_meter = '🟢S9'
		elif -99 <= self.rssi3 < -93:
			s_meter = '🟢S8'
		elif -105 <= self.rssi3 < -99:
			s_meter = '🟢S7'
		elif -111 <= self.rssi3 < -105:
			s_meter = '🟠S6'
		elif -117 <= self.rssi3 < -111:
			s_meter = '🟠S5'
		elif -123 <= self.rssi3 < -117:
			s_meter = '🟠S4'
		elif -129 <= self.rssi3 < -123:
			s_meter = '🟡S3'
		elif -135 <= self.rssi3 < -129:
			s_meter = '🟡S2'
		elif -141 <= self.rssi3 < -135:
			s_meter = '🔴S1'
		else:
			s_meter = '🔴S0'
		self.rssi = f'{s_meter} ({self.rssi3}dBm)'

	def __str__(self):
		"""Returns a string representation of the log line."""
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
		tg_map = self.data_manager.talkgroups.get_map()
		name = tg_map.get(tg_id_str)
		if not name and tg_id_str.isdigit():
			tg_id = int(tg_id_str)
			rules = self.data_manager.dmr_gateway.get_rules()
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
					name = f'{Formatter.get_flag_emoji(code)} {code}'
		if name:
			tg_name = f' ({name})'
		return tg_name

	def get_caller_location(self) -> str:
		"""Returns the location of the caller based on the callsign."""
		caller = ''
		user_map = self.data_manager.users.get_map()
		user_info = user_map.get(self.callsign)
		if user_info:
			call, fname, country = user_info
			code = Formatter.get_country_code(country)
			flag = Formatter.get_flag_emoji(code)
			label = code if code else country
			caller = f'~{call} ({fname}) [{flag} {label}]'
		elif self.callsign.isdigit() and len(self.callsign) == 7:
			mcc = int(self.callsign[:3])
			if mcc in MCC_CODES:
				_, code = MCC_CODES[mcc]
				caller = f' [{Formatter.get_flag_emoji(code)} {code}]'
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
		time = (self.timestamp.replace(tzinfo=dt.timezone.utc) or dt.datetime.now()).astimezone().isoformat(timespec='milliseconds')
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


class TelegramBot:
	"""Manages the Telegram bot application and message queue."""

	def __init__(self, token: str, chat_id: str, topic_id: str, app_name: str):
		self.token = token
		self.chat_id = chat_id
		self.topic_id = topic_id
		self.app: Optional[TelegramApplication] = None
		self.app_name = app_name
		self.queue = asyncio.Queue()

	async def queue_message(self, message: str):
		"""Queues a message to be sent."""
		await self.queue.put(message)

	async def run(self, stop_event: asyncio.Event):
		"""Runs the Telegram bot and message worker."""
		if not self.token:
			logging.error('Telegram token not provided. Bot will not start.')
			return

		try:
			self.app = ApplicationBuilder().token(self.token).build()
			logging.info('Telegram application built successfully.')
			async with self.app:
				await self.app.initialize()
				await self.app.start()
				logging.info('Telegram bot started successfully.')

				# Run the message worker
				worker_task = asyncio.create_task(self._worker(stop_event))
				await stop_event.wait()
				await worker_task

				await self.app.stop()
				await self.app.shutdown()
		except Exception as e:
			logging.error('Error running Telegram bot: %s', e)

	async def _worker(self, stop_event: asyncio.Event):
		"""Worker to process and send Telegram messages from the queue."""
		logging.info('Starting Telegram message worker...')
		while not stop_event.is_set():
			try:
				tg_message = await asyncio.wait_for(self.queue.get(), timeout=1.0)
			except asyncio.TimeoutError:
				continue
			message = f'{tg_message}\n\n<code>{self.app_name}</code>'
			if self.app:
				try:
					botmsg = await self.app.bot.send_message(
						chat_id=self.chat_id,
						message_thread_id=self.topic_id,
						text=message,
						parse_mode='HTML',
						link_preview_options={'is_disabled': True, 'prefer_small_media': True, 'show_above_text': True},
					)
					logging.info('Sent message to Telegram: %s/%s/%s', botmsg.chat_id, botmsg.message_thread_id, botmsg.message_id)
				except Exception as e:
					logging.error('Failed to send message to Telegram: %s', e)
			self.queue.task_done()
			await asyncio.sleep(0.5)


class LogObserver:
	"""Watches the MMDVM logs and sends updates via the Telegram bot."""

	def __init__(
		self,
		data_manager: DataManager,
		telegram_bot: TelegramBot,
		ignore_time_messages: bool = True,
		app_name_short: str = 'MMDVM_LastHeard',
		relevant_log_patterns: Optional[list[str]] = None,
	):
		self.data_manager = data_manager
		self.telegram_bot = telegram_bot
		self.log_reader = data_manager.log_reader
		self.ignore_time_messages = ignore_time_messages
		self.app_name_short = app_name_short
		self.relevant_log_patterns = relevant_log_patterns or []

	async def run(self, stop_event: asyncio.Event):
		"""Starts the log observation loop."""
		logging.info('Starting MMDVM log file retrieval...')
		last_event: Optional[dt.datetime] = None
		current_log_path: Optional[str] = None

		while not stop_event.is_set():
			try:
				latest_log = self.log_reader.get_latest_log_path()
				if current_log_path != latest_log:
					logging.info('Switching to new log file: %s', latest_log)
					if latest_log:
						msg = f'📃 {self.app_name_short} '
						if current_log_path:
							msg += (
								f'Log Changed\nOld File: <s>{os.path.basename(current_log_path)}</s>\nNew File: <b>{os.path.basename(latest_log)}</b>'
							)
						else:
							msg += f'Monitoring Log\nFile: <b>{os.path.basename(latest_log)}</b>'
						await self.telegram_bot.queue_message(msg)
					current_log_path = latest_log

				if current_log_path:
					await self._process_log_file(current_log_path, last_event)
					# Update last_event to avoid re-processing if needed,
					# but currently we just read the last line.
					# To properly track last_event, we'd need to return it from _process_log_file
					# or store it as instance state.
					# For now, let's read the last line and update state if it's new.
					last_line = self.log_reader.get_last_line(current_log_path)
					if any(pattern in last_line for pattern in self.relevant_log_patterns):
						try:
							parsed_line = MMDVMLogLine.from_logline(last_line, self.data_manager)
							if parsed_line.timestamp and (last_event is None or parsed_line.timestamp > last_event):
								last_event = parsed_line.timestamp
								await self._handle_new_entry(parsed_line)
						except ValueError:
							pass
				else:
					logging.error('No log file path available')
			except Exception as e:
				logging.error('Error in observer loop: %s', e)

			try:
				await asyncio.wait_for(stop_event.wait(), timeout=1.0)
			except asyncio.TimeoutError:
				pass

	async def _process_log_file(self, log_path: str, last_event: Optional[dt.datetime]):
		"""Processes a log file."""
		# This method is a placeholder if we wanted to read more than just the last line.
		# The logic is currently embedded in the run loop for the "last line" approach.
		pass

	async def _handle_new_entry(self, parsed_line: MMDVMLogLine):
		"""Handles a new log entry."""
		logging.info('New log entry: %s', parsed_line)
		if not (self.ignore_time_messages and '/TIME' in parsed_line.callsign):
			tg_message = parsed_line.get_telegram_message()
			if tg_message:
				await self.telegram_bot.queue_message(tg_message)
		elif self.ignore_time_messages:
			logging.info('Ignoring time message from gateway.')


async def main():
	"""Main function to initialize and run the Telegram bot and logs observer."""
	config = ConfigManager()
	data_manager = DataManager()
	telegram_bot = TelegramBot(config.tg_bot_token, config.tg_chat_id, config.tg_topic_id, config.app_name)
	log_observer = LogObserver(
		data_manager,
		telegram_bot,
		ignore_time_messages=config.gw_ignore_time_messages,
		app_name_short=config.app_name_short,
		relevant_log_patterns=config.relevant_log_patterns,
	)

	stop_event = asyncio.Event()
	loop = asyncio.get_running_loop()
	for sig in (signal.SIGINT, signal.SIGTERM):
		loop.add_signal_handler(sig, lambda: stop_event.set())

	# Start the Telegram bot task
	bot_task = asyncio.create_task(telegram_bot.run(stop_event))

	# Wait for bot to be ready (optional, but good for startup messages)
	# Since TelegramBot.run blocks, we run it in a task.
	# We can send a startup message immediately; it will sit in the queue until the bot connects.
	await telegram_bot.queue_message(f'🚀 {config.app_name_short} Started')

	# Start the log observer
	try:
		await log_observer.run(stop_event)
	except asyncio.CancelledError:
		logging.info('Main loop cancelled.')
	finally:
		await telegram_bot.queue_message(f'🛑 {config.app_name_short} Stopping')
		if not stop_event.is_set():
			stop_event.set()
		await bot_task


if __name__ == '__main__':
	LoggingManager().setup()
	try:
		logging.info('Starting the application...')
		asyncio.run(main())
	except KeyboardInterrupt:
		logging.info('Stopping application...')
	except Exception as e:
		logging.error('An error occurred: %s', e)
	finally:
		logging.info('Exiting script...')
