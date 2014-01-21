import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.font_manager import FontProperties
import json
import sys

arg_count = len(sys.argv)-1
if arg_count != 2:
	print "2 parameters, specifying files to compare, expected - %s given"%arg_count
	exit()

filename_1 = sys.argv[1]
filename_2 = sys.argv[2]

json_data_cypher=open(filename_1).read()
json_data_steps=open(filename_2).read()

query_data_cypher = json.loads(json_data_cypher)
query_data_steps = json.loads(json_data_steps)

time_90_cypher = [query["run_time"]["90th_percentile"] for query in query_data_cypher]
time_90_steps = [query["run_time"]["90th_percentile"] for query in query_data_steps]

query_names = [query["name"].split(".")[-1] for query in query_data_cypher]

ind = np.arange(0, 2*len(query_names), 2) 
w = 0.34       # the width of the bars

fig, ax = plt.subplots()

rects_90_cypher = ax.bar(ind, time_90_cypher, width=w, color='b', align='center', log=True)
rects_90_steps = ax.bar(ind+w, time_90_steps, width=w, color='r', align='center', log=True)

# TODO extract time unit from JSON
time_unit = query_data_cypher[0]["run_time"]["unit"]
ax.set_ylabel('Runtime (%s)'%time_unit)
ax.set_title('Query runtimes')
ax.set_xticks(ind+w+w+w/2)
ax.set_xticklabels(tuple(query_names))
ax.set_yscale('symlog')
ax.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
# ax.xaxis_date()
# ax.autoscale(tight=True)

fig.autofmt_xdate()

fontP = FontProperties()
fontP.set_size('small')
ax.legend( (rects_90_cypher[0], rects_90_steps[0]), ('90th Cypher', '90th Steps'),
	loc='upper left', fancybox=True, shadow=False, ncol=1, prop=fontP)#, bbox_to_anchor=(1.2, 1.0))

# attach some text labels
def autolabel(rects):
    for rect in rects:
		height = rect.get_height()
		if height == 0:
			ax.text(rect.get_x()+rect.get_width()/2., 0.8, '%d'%int(height), ha='center', va='top', rotation=90, size='small')
		else:
			ax.text(rect.get_x()+rect.get_width()/2., 0.9*height, '%d'%int(height), ha='center', va='top', rotation=90, size='small')

autolabel(rects_90_cypher)
autolabel(rects_90_steps)

# ax.autoscale_view()

# plt.xlim([0,len(query_names)])
y_upper = 1.1 * max(max(time_90_cypher),max(time_90_steps))
plt.ylim(ymax = y_upper, ymin = 0)
# plt.axis('tight')
plt.margins(0.05, 0.0)
# SET IMAGE SIZE - USEFUL FOR OUTSIDE LEGEND
# plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.14)
# plt.tight_layout()
plt.ylim(ymin = 0.1)
plt.savefig('result_compare.pdf')
plt.show()