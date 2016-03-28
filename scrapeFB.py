import urllib2
import json
import csv
import time
import StringIO
from datetime import datetime
import re

app_id = "1666699916952907"
app_secret = "8e838207a031c2b4fbde2634adafea22"
access_token = app_id + "|" + app_secret
page_id = 'opportunitiesforvnese'

def request_until_succeed(url):
    req = urllib2.Request(url)
    success = False
    while success is False:
        try: 
            response = urllib2.urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception, e:
            print e
            time.sleep(5)
            
            print "Error for URL %s: %s" % (url, datetime.now())

    return response.read()

base = "https://graph.facebook.com"
node = "/" + page_id + "/feed" 

def getFacebookPageFeedData(page_id, access_token, num_statuses):
    
    # construct the URL string
    parameters = "/?fields=message,link,type,name,likes{id},comments{from{id},message_tags},shares&limit=%s&access_token=%s" % (num_statuses, access_token) # changed
    url = base + node + parameters
    
    # retrieve data
    data = json.loads(request_until_succeed(url))
    
    return data

def processFacebookPageFeedStatus(status):
    

    status['message'] = '' if 'message' not in status.keys() else status['message']
    status['name'] = '' if 'name' not in status.keys() else status['name']
    status['link'] = '' if 'link' not in status.keys() else status['link']
    status['shares'] = 0 if 'shares' not in status.keys() else status['shares']['count']
    status['deadline'] = dict()
    status['labels'] = []

    buf = StringIO.StringIO(status['message'])
    line = buf.readline()
    while line == '\n':
        line = buf.readline()

    def shove_label(line):
        line = line.strip()
        label = ''
        open_label = line.find("[")
        close_label = line.find("]")
        if (open_label >= 0  and close_label > (open_label + 1)):
            label = line[(open_label+1):close_label]
            line = line[(close_label+1):].strip()
        if label != '': status['labels'].append(dict(name = label))
        return line

    line = shove_label(line)    
    subname = shove_label(status['name'])
    status['name'] = line
    status['subname'] = subname

    message = ''
    time_detected = False
    line = buf.readline()  
    while line != '':
    	if (line[0:11] == u'H\u1ea1n \u0111\u0103ng k\xed' or line[0:11] == u'H\u1ea1n \u0111\u0103ng k\xfd' or line[0:8] == u'Deadline' or line[0:8] == u'H\u1ea1n Ch\xf3t') and (not time_detected):
            time_detected = True
            sp = line.split()
            timestring = sp[len(sp)-1]
            try:
    			time = datetime.strptime(timestring,u'%d/%m/%Y')
            except:
    			try:
    				time = datetime.strptime(timestring, u'%d/%m/%Y.')
    			except:
    				try:
    					time = datetime.strptime(timestring,u'%d-%m-%Y')
    				except:
    					try:
    						time = datetime.strptime(timestring,u'%d/%m//%Y')
    					except:
    						try:
    							sp = line.split(':')
    							timestring = sp[len(sp)-1]
    							time = datetime.strptime(timestring,u'%d/%m/%Y\n')
    						except:
    							status['deadline']['Exception'] = line[12:].strip()
    							break
            status['deadline']['year'] = time.year
            status['deadline']['month'] = time.month
            status['deadline']['day'] = time.day
        else:
            message += line
    	line = buf.readline()

    message = re.sub(r'\n{2,}', '\n', message)
    status['message'] = message.strip()

    all_comments = set()
    all_tags = set()
    inv_map = dict()

    for comment in status['comments']:
        all_comments |= {comment['from']['id']}
        if 'message_tags' in comment.keys():
            for tag in comment['message_tags']:
                all_tags |= {tag['id']}
                if tag['id'] in inv_map:
                    inv_map[tag['id']] |= {comment['from']['id']}
                else:
                    inv_map[tag['id']] = {comment['from']['id']}

    comments = []
    for comment in all_comments:
        comments.append(dict(id = comment))
    tags = []
    for tag in all_tags:
        #print tag
        #print '--'
        #print inv_map
        tags.append(dict(id = tag, by = [dict(id = p) for p in inv_map[tag]]))

    status['comments'] = comments
    status['tags'] = tags

    return status

def augmentStatusLikes(status):

	has_next_page = True
	if 'likes' not in status.keys():
		status['likes'] = []
		return status
	this_page_likes = status['likes']
	all_likes = []
	while has_next_page:
		all_likes += this_page_likes['data']
		if 'paging' in this_page_likes.keys() and 'next' in this_page_likes['paging'].keys():
			this_page_likes = json.loads(request_until_succeed(this_page_likes['paging']['next']))
		else:
			has_next_page = False

	status['likes'] = all_likes
	return status

def augmentStatusComments(status):

	has_next_page = True
	if 'comments' not in status.keys():
		status['comments'] = []
		return status
	this_page_comments = status['comments']
	all_comments = []
	while has_next_page:
		all_comments += this_page_comments['data']
		if 'paging' in this_page_comments.keys() and 'next' in this_page_comments['paging'].keys():
			this_page_comments = json.loads(request_until_succeed(this_page_comments['paging']['next']))
		else:
			has_next_page = False

	status['comments'] = all_comments
	
	return status

eliminate = {}
eliminate |= {'1538152059731906_943872932358902',
    '1538152059731906_1553767038170408',
    '1538152059731906_1606792102867901',
    '1538152059731906_1612669365613508',
    '1538152059731906_1681357302078047',
    '1538152059731906_1551313788415733',
    '1538152059731906_416682255194315',
    '1538152059731906_120003498366392',
    '1538152059731906_1684159728464471',
    '1538152059731906_1681719112041866',
    '1538152059731906_548722925275575',
    '1538152059731906_124992874526861',
    '1538152059731906_515829341914875',
    '1538152059731906_1676380029242441',
    '1538152059731906_1205603519456668',
    '1538152059731906_1673538712859906',
    '1538152059731906_1673451196201991',
    '1538152059731906_919787161446091',
    '1538152059731906_902725986488273',
    '1538152059731906_614809228621410',
    '1538152059731906_858854624222400',
    '1538152059731906_1666400030240441',
    '1538152059731906_1654411364772641',
    '1538152059731906_1653803394833438',
    '1538152059731906_1644177169129394',
    '1538152059731906_1631034283777016',
    '1538152059731906_1624637507750027',
    '1538152059731906_1624622424418202',
    '1538152059731906_1620862248127553',
    '1538152059731906_1616665345213910',
    '1538152059731906_248347742006895',
    '1538152059731906_1611820265698418',
    '1538152059731906_1609364362610675',
    '1538152059731906_867651009962565',
    '1538152059731906_1598376263709485',
    '1538152059731906_1598215867058858',
    '1538152059731906_1598180927062352',
    '1538152059731906_1590307584516353',
    '1538152059731906_1583202555226856',
    '1538152059731906_721364827971646',
    '1538152059731906_1571634276383684',
    "1538152059731906_749653195109084",
    '1538152059731906_745014245579978',
    "1538152059731906_751807748226739",
    '1538152059731906_1563252260555219',
    '1538152059731906_749419861798861',
    '1538152059731906_1549296441950801',
    '1538152059731906_1548361095377669',
    '1538152059731906_1538154123065033'}


def scrapeFacebookPageFeedStatus(page_id, access_token):
    fjson = open('%s_facebook_statuses.txt' % page_id, 'wb')
    
    has_next_page = True
    num_processed = 0   # keep a count on how many we've processed
    scrape_starttime = datetime.now()
        
    print "Scraping %s Facebook Page: %s\n" % (page_id, scrape_starttime)
        
    statuses = getFacebookPageFeedData(page_id, access_token, 100)
        
    while has_next_page:
        for status in statuses['data']:
        	if (status['id'] in eliminate):
        		continue
        	status = augmentStatusLikes(status)
        	status = augmentStatusComments(status)
        	fjson.write(json.dumps(processFacebookPageFeedStatus(status), indent = 4, ensure_ascii=False).encode('utf-8'))
        	num_processed += 1
        	if num_processed % 100 == 0:
        		print "%s Statuses Processed: %s" % (num_processed, datetime.now())

        # if there is no next page, we're done.
        if 'paging' in statuses.keys():
            statuses = json.loads(request_until_succeed(statuses['paging']['next']))
        else:
            has_next_page = False
                
    fjson.close()
    print "\nDone!\n%s Statuses Processed in %s" % (num_processed, datetime.now() - scrape_starttime)

