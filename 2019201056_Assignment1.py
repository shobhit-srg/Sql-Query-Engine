import csv
import sys
import re
from collections import OrderedDict

# read the information about tables
def readMetadata():
	dictionary = {}
	# open the file where table info is stored
	f = open('./metadata.txt','r') 
	check = 0
	if not f:
		return
	for line in f:
		# check if a new table entry has started
		if line.strip() == "<begin_table>":
			check = 1
			begin = "yes"
			continue
		if check == 1:
			tab_name = line.strip()
			# declare a new entry in dictionary for new table
			dictionary[tab_name] = []
			middle = "yes"
			check = 0
			continue
		# check if line is other than end of table
		if not line.strip() == '<end_table>':
			# add line to dictionary
			dictionary[tab_name].append(line.strip()); 
	return dictionary

def is_valid_query(query):
	if(len(query)<1):
		return False
	return True
# processing of every query begins from here
def processQuery(query,dictionary):
	# remove whitespaces from end of line
	query = query.strip()

	# declare a list to store all the tokens of the query
	query_words = []
	# check if query starts with "select"
	if "select" not in query.lower():
		sys.exit("Incorrect Syntax")

	# check if query contains "from"
	if "from" not in query.lower():
		sys.exit("Incorrect Syntax")
	else:
		div_at_from = query.split('from')

	div_at_from[0] = div_at_from[0].strip()

	if "select" not in div_at_from[0].lower():
		vq = is_valid_query(query)
		sys.exit("Incorrect Syntax")
	sel_to_from = div_at_from[0][7:]
	if len(query_words) > 1:
		return 
	query_words.append("select")
	sel_to_from = sel_to_from.strip();

	if "distinct" in sel_to_from:
		sel_to_from = sel_to_from[9:]
		query_words.append("distinct")

	if len(query_words) > 3:
		return
	query_words.append(sel_to_from)

	# select distinct List<col_names> from <table>
	distinct = ""
	if "distinct" in query_words[1]:
		distinct = query_words[1];
		distinct = distinct.strip()
		# if distinct appears in the query store it separately and the rest
		# of the query to positon 1 so that further evaluation is generalized
		query_words[1] = query_words[2]

	colStr = query_words[1]; 
	colStr = colStr.strip()
	col_names = colStr.split(',');
	for i in range(len(col_names)):
		col_names[i] = col_names[i].strip()

	div_at_from[1] = div_at_from[1].strip()
	# the below line would be used to obtain the name of tables in query
	from_to_where = div_at_from[1].split('where');
	vq = is_valid_query(query)
	tableStr = from_to_where[0]
	if vq:
		tableStr = tableStr.strip()
	# store the name of tables in a list
	tab_names = tableStr.split(',')


	for i in range(len(tab_names)):
		tab_names[i] = tab_names[i].strip()
	for i in tab_names:
		if i not in dictionary.keys():
			sys.exit("Error: table not found")

	done = 0

	# Select with where from one or more tables
	if len(from_to_where) > 1 and len(tab_names) == 1:
		from_to_where[1] = from_to_where[1].strip()
		Where(from_to_where[1],col_names,tab_names,dictionary)
		done = 1
		return
	elif(len(tab_names) > 1):
		if len(from_to_where) > 1:
			from_to_where[1] = from_to_where[1].strip()
			WhereJoin(from_to_where[1],col_names,tab_names,dictionary)
			done = 1
			return
		join(col_names,tab_names,dictionary)
		done = 1
	# Select/project with distinct from one table
	elif distinct == "distinct":
		distinctMany(col_names,tab_names,dictionary)
		done = 1
	elif len(col_names) == 1:
		#aggregate -- Assuming (len(col_names) == 1) i.e aggregate function
		for col in col_names:
			if '(' in col and ')' in col:
				a1 = col.split('(');
				funcName = a1[0].strip()
				col_name = a1[1].split(')')[0].strip()
				aggregate(funcName,col_name,tab_names[0],dictionary)
				done = 1
				return
			# there should be an error if there is only one of the parentheses bracket in the query
			elif '(' in col or ')' in col:
				sys.exit("Error: bracket missing in aggr function")
				done = 1

	if done == 0:
		selectColumns(col_names,tab_names,dictionary)
	return
def check_validity(col_names,tab_names):
	if(len(col_names)<1):
		return False
	if(len(tab_names)<1):
		return False
	return True

def Where(where_cond,col_names,tab_names,dictionary):
	# make a list of all the tokens of the where condition
	a = where_cond.split(" ")

	# check if query requires all columns from all tables
	if(col_names[0] == '*' and len(col_names) == 1):
		col_names = dictionary[tab_names[0]]

	valid = check_validity(col_names,tab_names)

	# print the desired column names
	printHeader(col_names,tab_names,dictionary)

	tName = tab_names[0] + '.csv'
	fileData = []
	valid = check_validity(col_names,tab_names)
	readFile(tName,fileData)

	check = 0
	if not valid:
		return
	for data in fileData:
		# construct a string out of the given column names and table names
		string = evaluate(a,tab_names,dictionary,data)
		for col in col_names:
			# evaluate the string with the python function eval()
			if valid and eval(string):
				valid = check_validity(col_names,tab_names)
				check = 1 
				# print comma separated desired column values
				print (data[dictionary[tab_names[0]].index(col)],end=",")
		# add a new line when one row is finished processing
		if valid and check == 1:
			t = True
			check = 0
			print ()

def check_tab(tab_names):
	if(len(tab_names)<1):
		return False
	return True

# function to construct the actual query 
def evaluate(a,tab_names,dictionary,data):
	string = ""
	
	t = check_tab(tab_names)

	for i in a:
		if t and i == '=':
			string += "=="
		elif t and i in dictionary[tab_names[0]] :
			string += data[dictionary[tab_names[0]].index(i)]
		elif i.lower() == 'and' or i.lower() == 'or':
			s = i.lower()
			t = t
			string += ' ' + s  + ' '
		else:
			string += i
	return string 

def WhereJoin(where_cond,col_names,tab_names,dictionary):
	tab_names.reverse()

	# read data from required table files
	l1 = []
	l2 = []
	valid = check_validity(col_names,tab_names)
	readFile(tab_names[0] + '.csv',l1)
	readFile(tab_names[1] + '.csv',l2)
	if not valid:
		return
	fileData = []
	for item1 in l1:
		valid = check_tab(tab_names)
		for item2 in l2:
			fileData.append(item2 + item1)

	# construct dictionary["sample"] for join and where conditions
	dictionary["sample"] = []
	for x in [1,0]:
		for i in dictionary[tab_names[x]]:
			dictionary["sample"].append(tab_names[x] + '.' + i)

	if valid:
		dictionary["test"] = dictionary[tab_names[1]] + dictionary[tab_names[0]]

	tab_names.remove(tab_names[0])
	if not valid:
		return
	# add "sample" as table name in tab_names list
	tab_names.insert(0,"sample")

	# check whether it is required to print all columns in the 
	if(col_names[0] == '*'):
		if(len(col_names) == 1):
			col_names = dictionary[tab_names[0]]
	t = check_validity(col_names,tab_names)
	# print column names of desired columns
	for i in col_names:
		print (i,end=",")
	# print new line at the end of header
	print()

	if not t:
		return

	# make a list of all the tokens of the where condition
	a = where_cond.split(" ")

	check = 0
	for data in fileData:
		# construct a string out of the given column names and table names
		string = evaluate(a,tab_names,dictionary,data)
		for col in col_names:
			# evaluate the string with the python function eval() for desired column names
			if t and eval(string):
				check = 1
				if '.' in col:
					valid = check_validity(col_names,tab_names)
					print (data[dictionary[tab_names[0]].index(col)],end=",")
				else:
					print (data[dictionary["test"].index(col)],end=",")
		if valid and check == 1:
			check = 0
			print()

	# delete sample from dictionary once work is done
	sample = dictionary.pop('sample',None)
	return 

def join(col_names,tab_names,dictionary):
	tab_names.reverse()

	# read data from required table files
	l1 = []
	l2 = []
	valid = check_validity(col_names,tab_names)
	readFile(tab_names[0] + '.csv',l1)
	readFile(tab_names[1] + '.csv',l2)

	fileData = []
	if not valid:
		return
	for item1 in l1:
		for item2 in l2:
			fileData.append(item2 + item1)

	# construct dictionary["sample"] for join and where conditions
	dictionary["sample"] = []
	for x in [1,0]:
		for i in dictionary[tab_names[x]]:
			dictionary["sample"].append(tab_names[x] + '.' + i)
	t = check_validity(col_names,tab_names)
	dictionary["test"] = dictionary[tab_names[1]] + dictionary[tab_names[0]]
	if not t:
		return
	tab_names.remove(tab_names[0])
	# add "sample" as table name in tab_names list
	tab_names.insert(0,"sample")

	if(len(col_names)<1):
		return

	# check whether it is required to print all columns in the 
	if(col_names[0] == '*'):
		if(len(col_names) == 1):
			col_names = dictionary[tab_names[0]]

	t = check_validity(col_names,tab_names)
	# print column names of desired columns
	for i in col_names:
		print (i,end=",")
	# print new line at the end of header
	print()
	if not t:
		return
	for data in fileData:
		for col in col_names:
			if t and '.' in col:
				valid = check_validity(col_names,tab_names)
				print (data[dictionary[tab_names[0]].index(col)],end=",")
			else:
				print (data[dictionary["test"].index(col)],end=",")
		print()

	# delete sample from dictionary once work is done
	sample = dictionary.pop('sample',None)
	return 

# a common function to print output headers, i.e. table names
def printHeader(col_names,tab_names,dictionary):
	string = ""
	t = check_validity(col_names,tab_names)
	for col in col_names:
		for tab in tab_names:
			if t and col in dictionary[tab]:
				t = t or t
				if not string == "":
					string += '  '
				string += tab + '.' + col
	print ("OUTPUT : \n" + string)

# print data from the costructed list of file rows 
def printData(fileData,col_names,tab_names,dictionary):
	valid = check_validity(col_names,tab_names)
	for data in fileData:
		for col in col_names:
			# print data of required columns only
			print (data[dictionary[tab_names[0]].index(col)],end=",")
		print()

# open file for reading, tables are stored in table.csv format 
def readFile(tName,fileData):
	with open(tName,'rt') as f:
		reader = csv.reader(f)
		# make a list out of file rows
		for row in reader:
			if row:
				fileData.append(row)

def distinct(colList,col_name,tab_name,dictionary):
	t = check_validity(col_names,tab_names)
	print ("OUTPUT :")
	string = tab_name + '.' + col_name
	print (string)
	if not t:
		return
	colList = list(OrderedDict.fromkeys(colList))
	for col in range(len(colList)):
		t = t or t
		print (colList[col])

# query: Select distinct col1, col2 from table_name;
def distinctMany(col_names,tab_names,dictionary):
	for col in col_names:
		print(col)
		if col not in dictionary[tab_names[0]]:
			sys.exit("Error: column not in table")

	printHeader(col_names,tab_names,dictionary) 

	temp = set()
	result_list = []
	valid = check_validity(col_names,tab_names)
	for tab in tab_names:
		tName = tab + '.csv'
		with open(tName,'rt') as f:
			reader = csv.reader(f)
			if not valid:
				return
			for row in reader:
				t = tuple()
				for col in col_names:
					t = t + (row[dictionary[tab_names[0]].index(col)],)
				if valid and t not in temp:
					temp.add(t)
					result_list.append(t)
	for t in result_list:
		for e in t:
			print (e,end=",")
		print()

# Project Columns(could be any number of columns) from one or more tables
def selectColumns(col_names,tab_names,dictionary):

	t = check_validity(col_names,tab_names)
	
	if len(col_names) == 1 and col_names[0] == '*':
		col_names = dictionary[tab_names[0]]
	if not t:
		return
	for i in col_names:
		if t and i not in dictionary[tab_names[0]]:
			sys.exit("Error: Incorrect column name")

	tName = tab_names[0] + '.csv'
	fileData = []
	readFile(tName,fileData)

	printHeader(col_names,tab_names,dictionary)	
	printData(fileData,col_names,tab_names,dictionary)

# Aggregate functions: Simple aggregate functions on a single column.
def aggregate(func,col_name,tab_name,dictionary):

	if col_name not in dictionary[tab_name]:
		sys.exit("Error: Incorrect column name")
	# a=make sure there is only one column
	if col_name == '*':
		sys.exit("Error: Functions work for single column only")

	tName = tab_name + '.csv'
	fileData = []
	readFile(tName,fileData)
	colList = []
	valid = check_validity(col_name,tab_name)
	for data in fileData:
		if valid:
			colList.append(int(data[dictionary[tab_name].index(col_name)]))

	if func.lower() == 'min':
		print (min(colList))
	elif valid and func.lower() == 'max':
		print (max(colList))
	elif func.lower() == 'avg':
		print (sum(colList)/len(colList))
	elif func.lower() == 'distinct':
		distinct(colList,col_name,tab_name,dictionary)
	elif func.lower() == 'sum':
		print (sum(colList))
	else :
		print ("ERROR: Unknown function : ", "'" + func + "'")


def main():
	dictionary = readMetadata()
	print (dictionary)
	query = str(sys.argv[1])
	# print(query)
	if query[-1] != ';':
		print ("Error: ';' missing")
	else:
		query = query[:-1]
		query = query.strip()
		# print(query)
		if query == "exit":
			print( "Exiting from SQL prompt...")
			return
		processQuery(query,dictionary)

main()