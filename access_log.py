import json
import re
#pattern = r'^(\S+) \S+ \S+ \[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2} [+\-]\d{4})\] "(\S+ \S+ \S+)" (\d+) (\d+) "(\S+)" "([^"]+)" (\d+)'

pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d+) (\d+) "([^"]*)" "([^"]*)" (\d+)'
# parsing json string
def parse_json_string(json_string):
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        return None
        
# parse logline into columns        
def parse_log(line):
    # remove new line character from the log line
    line = re.sub(r'\n$', '', line)
    match = re.search(pattern, str(line))
    if match is None:
        raise Error("Invalid logline: %s" % line)

    return {
        "ip_address": match.group(1),
	"remote_log_name": match.group(2),
	"user_id": match.group(3),
	"log_timestamp": match.group(4),
	"request": match.group(5),
	"response_code": int(match.group(6)),
	"response_bytes": int(match.group(7)),
	"referrer": match.group(8),
	"user_agent": match.group(9),
	"response_time": int(match.group(10))
    }


