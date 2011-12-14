# Altai State University, Pavel Nuzhdin, 2011
import random, math

# Attributes
fieldseparator = ',' # Fields separator
datafilepath = "../data/covtype.data" # Data file
datafileoutpath = "../data/covtype-trainset.data" # Data file for output
classfieldnum = 54 # class (Y) field position
# Class (Y) distribution
classdistributions = {'1': 211840, '2': 283301, '3': 35754, '4': 2747, '5': 9493, '6': 17367, '7': 20510}

# Algo
rowscount = reduce(lambda x, y: x + y, classdistributions.values())
for trainpercent in xrange(5,100,5):
	f = open(datafilepath, 'r+')
	fout = open(datafileoutpath + '-' + str(trainpercent), 'w+')
	classneeded = dict(map(lambda x: (x[0], int(math.ceil(float(x[1])*trainpercent / 100))), classdistributions.items()))
	print "trainpercent: " + str(trainpercent) + ", classneeded: " + str(classneeded)
	for line in f:
		if reduce(lambda x, y: x + y, classneeded.values()) == 0:
			break
		if random.randint(1, 100) == 1:
			continue
		fields = line.rstrip('\n').split(fieldseparator)
		curclass = fields[classfieldnum]
		if classneeded[curclass] > 0:
			classneeded[curclass] = classneeded[curclass] - 1
		fout.write(line)
	fout.close()
f.close()