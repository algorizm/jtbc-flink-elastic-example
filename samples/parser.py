import csv
import datetime
import re
import time


csv_file_name = './ssl-access-jtbctk.log-20220621.csv'
log_file_name = './jtbcv126/ssl-access-jtbctk.log-20220621'

log_file = open(log_file_name, encoding='utf-8')
pattern = re.compile(r"""(?P<remote>[^\s-]*) ([^\[]*) \[(?P<reqtime>[^\]]*)\] [^"]* "(?P<method>\S+) (?P<path>(\/\?)(?P<parms>[^\s]*))\s.[^"]*" (?P<code>[^\s]*) (?P<size>[^\s]*) "([^"]*)+" \"(?P<agent>[^"]*)\" \"(?P<referer>[^"]*)\" \"(?P<xforward>[^"]*)\" \"(?P<reqbody>[^"]*)\"[^\"\"]""", re.IGNORECASE)
total = 0

with open(csv_file_name, 'w') as out:
	csv_out = csv.writer(out)
	try:
		for line in log_file.readlines():
			#print(line)
			total = total + 1
			data = re.search(pattern, line)
			if data is not None:
				datadict = data.groupdict()
				reqtime = datetime.datetime.strptime(datadict['reqtime'], '%Y-%m-%dT%H:%M:%S+09:00').strp
				print(datadict['remote'], reqtime.strftime('%Y-%m-%d %H:%M:%S'))
				csv_out.writerow([datadict['remote'], datadict['reqtime']])

			if total >= 1:
				break
				#csv_out.writerow({datadict['remote'], datadict['reqtime']})
			'''
expression /(?<remote>[^\s-]*) ([^\[]*) \[(?<reqtime>[^\]]*)\] [^"]* "(?<method>\S+) (?<path>(\/\?)(?<parms>[^\s]*))\s.[^"]*" (?<code>[^\s]*) (?<size>[^\s]*) "([^"]*)+" "(?<agent>[^"]*)" "(?<referer>[^"]*)" "(?<xforward>[^"]*)" "(?<reqbody>[^"]*)"/			
			'''
	except Exception as e:
		print(e)

# import sys
# sys.exit()
# with open(csv_file_name, 'w') as out:
# 	csv_out = csv.writer(out)
# 	# csv_out.writerow(['remote', 'reqtime'])
# 	# csv_out.writerow(['remote', 'method', 'reqtime', 'code', 'size', 'agent', 'referer', 'xforward', 'reqbody'])
#
