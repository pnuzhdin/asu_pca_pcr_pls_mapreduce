# Altai State University, Pavel Nuzhdin, 2011
import random, math

# Attributes
fieldseparator = ',' # Fields separator
datafilepath = "../data/yearprediction/yearprediction-training-pca" # Data file
datafileoutpath = "../data/yearprediction/yearprediction-training-pca-samples" # Data file for output
classfieldnum = 0 # class (Y) field position
# Class (Y) distribution
classes = range(1922, 2012)
rowscount = 463715 # Rows count
maxrowsbyclass = 3 # Maximum rows by each class

f = open(datafilepath, 'r+')
fout = open(datafileoutpath, 'w+')
classneeded = dict( map(lambda x: (str(x), maxrowsbyclass), classes))
for line in f:
	if reduce(lambda x, y: x + y, classneeded.values()) == 0:
		break
	#if random.randint(1, 100) == 1:
	#	continue
	fields = line.rstrip('\n').split(fieldseparator)
	curclass = fields[classfieldnum]
	if classneeded[curclass] > 0:
		classneeded[curclass] = classneeded[curclass] - 1
		fout.write(line)
fout.close()
f.close();