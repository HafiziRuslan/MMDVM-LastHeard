#!/usr/bin/python3
"""
Test script to process MMDVM log files and display what would be sent to Telegram.
This script loads the MMDVMLogLine class from main.py and processes log entries.
"""

import os
import sys

# Mock telegram modules before importing main
import unittest.mock as mock

sys.modules['telegram'] = mock.MagicMock()
sys.modules['telegram.ext'] = mock.MagicMock()

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Now import from main.py
try:
	import importlib.util

	# Load main.py as a module
	spec = importlib.util.spec_from_file_location('main', 'main.py')
	main_module = importlib.util.module_from_spec(spec)

	# Inject required dependencies
	import asyncio
	import logging
	import threading

	main_module.threading = threading
	main_module.logging = logging
	main_module.os = os
	main_module.glob = __import__('glob')
	main_module.asyncio = asyncio
	main_module.load_dotenv = lambda: None  # Mock load_dotenv

	# Execute the module
	spec.loader.exec_module(main_module)

	# Import the class we need
	MMDVMLogLine = main_module.MMDVMLogLine

	print('✅ Successfully loaded MMDVMLogLine class from main.py\n')

except Exception as e:
	print(f'❌ Error loading main.py: {e}')
	print('Make sure main.py is in the current directory.')
	sys.exit(1)


def process_log_file(log_file_path: str, ignore_time_messages: bool = True):
	"""
	Process a log file and display all entries that would be sent to Telegram.

	Args:
	        log_file_path: Path to the MMDVM log file
	        ignore_time_messages: Whether to ignore /TIME messages (default: True)
	"""

	# Patterns to match (same as in main.py)
	relevant_patterns = [
		'end of voice transmission',
		'end of transmission',
		'watchdog has expired',
		'received RF data',
		'received network data',
		'received network Data',
		'ended RF data transmission',
		'ended network data transmission',
	]

	print('=' * 80)
	print(f'Processing log file: {log_file_path}')
	print('=' * 80)
	print()

	if not os.path.exists(log_file_path):
		print(f'❌ Error: File not found: {log_file_path}')
		return

	total_lines = 0
	matched_lines = 0
	parsed_entries = 0
	telegram_messages = 0
	last_timestamp = None

	entries = []

	try:
		with open(log_file_path, 'r', encoding='UTF-8', errors='replace') as f:
			for line_num, line in enumerate(f, 1):
				total_lines += 1
				line = line.strip()

				# Skip empty lines
				if not line or len(line) < 10:
					continue

				# Check if line matches our patterns
				if not any(pattern in line for pattern in relevant_patterns):
					continue

				matched_lines += 1

				try:
					# Parse the log line
					parsed = MMDVMLogLine.from_logline(line)
					parsed_entries += 1

					# Check if this is a duplicate timestamp (same as last processed)
					if last_timestamp and parsed.timestamp <= last_timestamp:
						continue

					last_timestamp = parsed.timestamp

					# Check if we should ignore this message
					if ignore_time_messages and '/TIME' in parsed.callsign:
						continue

					# Get the Telegram message
					tg_message = parsed.get_telegram_message()

					if tg_message:
						telegram_messages += 1
						entries.append({'line_num': line_num, 'parsed': parsed, 'message': tg_message})

				except ValueError:
					# Line didn't match any pattern
					pass
				except Exception as e:
					print(f'⚠️ Warning at line {line_num}: {e}')

	except Exception as e:
		print(f'❌ Error reading file: {e}')
		return

	# Display statistics
	print('📊 Statistics:')
	print(f'  Total lines in file: {total_lines}')
	print(f'  Lines matching patterns: {matched_lines}')
	print(f'  Successfully parsed entries: {parsed_entries}')
	print(f'  Messages for Telegram: {telegram_messages}')
	print()

	if telegram_messages == 0:
		print('ℹ️ No messages would be sent to Telegram from this log file.')
		return

	# Display all messages
	print('=' * 80)
	print(f'📱 TELEGRAM MESSAGES ({telegram_messages} total)')
	print('=' * 80)
	print()

	for idx, entry in enumerate(entries, 1):
		print(f'{"─" * 80}')
		print(f'Message #{idx} (from line {entry["line_num"]})')
		print(f'{"─" * 80}')

		parsed = entry['parsed']
		print(f'📍 Mode: {parsed.mode}')
		print(f'📍 Callsign: {parsed.callsign}')
		print(f'📍 Destination: {parsed.destination}')
		print(f'📍 Timestamp: {parsed.timestamp}')
		print(f'📍 Network: {"Yes" if parsed.is_network else "No (RF)"}')
		print()
		print('Telegram Message (HTML format):')
		print('┌' + '─' * 78 + '┐')
		for line in entry['message'].split('\n'):
			print(f'│ {line:<76} │')
		print('└' + '─' * 78 + '┘')
		print()

	print('=' * 80)
	print('✅ Processing complete!')
	print('=' * 80)


def main():
	"""Main function"""
	import argparse

	parser = argparse.ArgumentParser(
		description='Test MMDVM log processing and display Telegram messages',
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog="""
Examples:
 %(prog)s MMDVM-2026-01-01.log
 %(prog)s MMDVM-2026-01-01.log --include-time
 %(prog)s /var/log/pi-star/MMDVM-2026-01-01.log
		""",
	)

	parser.add_argument('logfile', help='Path to the MMDVM log file to process')

	parser.add_argument('--include-time', action='store_true', help='Include /TIME messages (by default they are ignored)')

	args = parser.parse_args()

	# Process the log file
	process_log_file(args.logfile, ignore_time_messages=not args.include_time)


if __name__ == '__main__':
	# If no arguments provided, show help
	if len(sys.argv) == 1:
		print('MMDVM Log Processor - Test Tool')
		print('=' * 80)
		print()
		print('Usage: python test_log_processor.py <logfile> [--include-time]')
		print()
		print('This script processes an MMDVM log file and displays all entries')
		print('that would be sent to the Telegram channel.')
		print()
		print('Examples:')
		print(' python test_log_processor.py MMDVM-2026-01-01.log')
		print(' python test_log_processor.py MMDVM-2026-01-01.log --include-time')
		print()

		# If MMDVM-2026-01-01.log exists in current directory, use it as default
		default_log = 'MMDVM-2026-01-01.log'
		if os.path.exists(default_log):
			print(f'ℹ️ Found log file: {default_log}')
			print('  Processing it now...\n')
			process_log_file(default_log)
		else:
			print('❌ No log file specified and MMDVM-2026-01-01.log not found.')
			print()
			sys.exit(1)
	else:
		main()
