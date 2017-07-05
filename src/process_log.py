import time, sys
from math import sqrt

# Queue Implementation
class Queue:
    def __init__(self):
	self.items = []

    # checks if Queue is empty
    def isEmpty(self):
	return self.items == []

    # inserts element to Queue
    def enqueue(self,item):
	self.items.insert(0,item)

    # removes element from Queue
    def dequeue(self):
	return self.items.pop()

    # gets the size of the Queue
    def size(self):
	return len(self.items)


# convert date time string to epoch time
# e.g., "timestamp":"2017-06-13 11:33:01"
def convertToEpoch(dateString):
    pattern = '%Y-%m-%d %H:%M:%S'
    return int(time.mktime(time.strptime(dateString, pattern)))

# calculates mean of a given list
def mean(l):
    m = sum(l) / float(len(l))
    return m

# calculates standard deviation of a given list
def sd(l):
    m = mean(l)
    diff = [x-m for x in l]
    sq_diff = [d**2 for d in diff]
    variance = sum(sq_diff)/len(l)
    return sqrt(variance)

# User class represents node of graph
class User:
    def __init__(self,key):
	self.id = key
	self.connectedTo = {}
	self.purchaseHistory = []
  
    # add neighbor/ friend to a given user node
    def addFriend(self,frd,weight=1):
	self.connectedTo[frd] = weight

    # removes neighbor/ friend for a given user node
    def removeFriend(self,frd):
	if frd in self.getConnections():
	    del self.connectedTo[frd]

    # string representation of user object
    def __str__(self):
	return str(self.id) + ' connectedTo: ' + str([x.id for x in self.connectedTo])

    # get the neighbors/ friends of a given user
    def getConnections(self):
	return self.connectedTo.keys()

    # get the userid for a given user
    def getId(self):
	return self.id

    # get the weight assigned to the friend of a user
    def getWeight(self,frd):
	return self.connectedTo[frd]

    # update the purchase history for a given user
    def updatePurchaseHistory(self,ts,amt):
	epochTime = convertToEpoch(ts)
        self.purchaseHistory.insert(0,(epochTime, amt))

    # get the purchase history for a given user
    def getPurchaseHistory(self):
	return self.purchaseHistory


# represents the Social Network/ Graph of user nodes
class SocialNetwork:
    def __init__(self):
        self.userList = {}
        self.numUsers = 0

    # add user node to social network/ graph
    def addUser(self,key):
	if self.getUser(key) is None:
	    self.numUsers = self.numUsers + 1
	    newUser = User(key)
	    self.userList[key] = newUser
	    return newUser

    # update user's purchase history
    def updateUser(self,usr,ts,amt):
	u = self.getUser(usr)
	if u is not None:
	    u.updatePurchaseHistory(ts,amt)    

    # get user node in the social network/ graph
    def getUser(self,usr):
	if usr in self.userList:
	    return self.userList[usr]
	else:
	    return None

    # get the number of user nodes in the social network/ graph
    def getUserCount(self):
	return self.numUsers

    # checks if user is present in social network/ graph
    def __contains__(self,usr):
	return usr in self.userList

    # add edge between 2 friends
    def addEdge(self,usr1,usr2,degree=1):
	# create user if not present
	if usr1 not in self.userList:
	    nu = self.addUser(usr1)
	if usr2 not in self.userList:
	    nu = self.addUser(usr2)
	self.userList[usr1].addFriend(self.userList[usr2], degree)
	self.userList[usr2].addFriend(self.userList[usr1], degree)

    # remove edge if users are not friends anymore
    def removeEdge(self,usr1,usr2):
	if usr1 in self.userList and usr2 in self.userList:
	    self.userList[usr1].removeFriend(self.userList[usr2])
	    self.userList[usr2].removeFriend(self.userList[usr1])

    # get list of users
    def getUsers(self):
	return self.userList.keys()

    # get friends of a given user at a given degree
    def getUserFriends(self,usr,degree):
	d = 1
	friends = []		# friends of a given user
	visited = []		# track already visited user nodes
	u = self.getUser(usr)
	if u is not None:
	    q = Queue()
	    q.enqueue(u)
	    q.enqueue(None)
	    while d > 0 and d <= degree and q.size() > 0:	# condition to check friends until given degree
		currUser = q.dequeue()
		if currUser is not None:
		    for x in currUser.getConnections():		# loop through current user connections
			if x.id != usr and x.id not in visited:	# check if it is not the given user and not already visited
		            friends.append(x)
		            q.enqueue(x)
			    visited.append(x.id)
		else:
		    d += 1
		    q.enqueue(None)
	return friends

    def __iter__(self):
	return iter(self.userList.values())

if __name__ == '__main__':
    bufsize = 65536
    batchFile = sys.argv[1]
    streamFile = sys.argv[2]
    outputFile = sys.argv[3]
    sn = SocialNetwork()
    D = 0
    T = 0
    friends = []
    purchases = []
    m = 0.0
    sdev = 0.0
    target = 0.0
    fo = open(outputFile, "w+")          # open outfile for writing results
    fo.seek(0, 0)                        # position at beginning of output file
    
    # process the contents in batch log file
    with open(batchFile) as infile1:
        while True:
	    # read lines of the batch log file
	    lines = infile1.readlines(bufsize)
	    if not lines:
	        break
	    for line in lines:
		# replace and split lines to get the entry values
		entries = line.replace('{','').replace('}','').replace('"','').replace('\n','').split(',')
		# get values in the first line of batch log file
		if entries[0].split(':')[0] == 'D':
		    D = int(entries[0].split(':')[1])
		    T = int(entries[1].split(':')[1])		
		else:
		    # eventType - purchase or befriend or unfriend
		    eventType = entries[0].split(':')[1]
		    # timestamp value
		    ts = entries[1].split('timestamp:')[1]
		    if eventType == 'purchase':
			# user id
		        id = entries[2].replace(' ','').split(':')[1]
			# purchase amount
		        amt = float(entries[3].replace(' ','').split(':')[1])
			# add user to the graph
		        sn.addUser(id)
			# update user's purchase history
			sn.updateUser(id,ts,amt)
		    else:
			# id's of befriend and unfriend events
		        id1 = entries[2].replace(' ','').split(':')[1]
		        id2 = entries[3].replace(' ','').split(':')[1]
		        if eventType == 'befriend':
			    # add edge for friends
		            sn.addEdge(id1,id2)
		        elif eventType == 'unfriend':
			    # remove edge if not friends
			    sn.removeEdge(id1,id2)

    # process contents in the stream log file
    with open(streamFile) as infile2:
        while True:
	    # read lines of the file
	    lines = infile2.readlines(bufsize)
	    if not lines:
	        break
	    for line in lines:
		# replace and split lines to get the entry values
		entries = line.replace('{','').replace('}','').replace('"','').replace('\n','').split(',')
		# eventType - purchase or befriend or unfriend
		eventType = entries[0].split(':')[1]
		# timestamp entry value
		ts = entries[1].split('timestamp:')[1]
		if eventType == 'purchase':
		    # user id
		    id = entries[2].replace(' ','').split(':')[1]
		    # purchase amount
		    amt = float(entries[3].replace(' ','').split(':')[1])
		    # add user to the graph
		    sn.addUser(id)
		    # update user's purchase history
		    sn.updateUser(id,ts,amt)
		else:
		    # id's of befriend and unfriend events
		    id1 = entries[2].replace(' ','').split(':')[1]
		    id2 = entries[3].replace(' ','').split(':')[1]
		    if eventType == 'befriend':
			# add edge for friends
			sn.addEdge(id1,id2)
		    elif eventType == 'unfriend':
			# remove edge if not friends
			sn.removeEdge(id1,id2)
		# calculate mean and sd if eventType='purchase' for the line entry of stream log file
		if D!=0 and T!=0 and eventType == 'purchase':
		    # get friends of the user in the line entry of stream log file
		    friends = sn.getUserFriends(id,D)
		    if len(friends) > 0:
		        purchases = [x.getPurchaseHistory() for x in friends]
		        purchases = [j for i in purchases for j in i]
			if len(purchases) > 0:
			    # sort purchase values as per timestamp value in the tuple
		            purchases.sort(key=lambda x: x[0],reverse=True)
			    # get until T purchases amounts
		            purchases = purchases[:T]
			    # get only the purchase amount values
			    purchases = [x[1] for x in purchases]
			    # calculate mean of list of purchases
			    m = mean(purchases)
			    # truncate mean to 2 decimals
			    m = m-(m%0.01)
			    # calculate standard deviation of list of purchases
			    sdev = sd(purchases)
			    # standard deviation truncated to 2 decimals
			    sdev = sdev-(sdev%0.01)
			    target = m + (3*sdev)  
			    # check if purchase amount < mean+3*sd
			    if amt > target:
				s = ', "mean": "' + ('%.2f' % m) + '", "sd": "' + str('%.2f' % sdev) + '"}'
				line = line.replace('}',s)
			        fo.write(line)                   # write purchases with anomalous values to output file
			        fo.write("\n")                   # write a new line to output file
			        fo.seek(0, 2)                    # go to end of file
    fo.close()						     	 # close the output file
