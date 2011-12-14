# Altai State University, Pavel Nuzhdin, 2011
from numpy import random, linalg, dot, array, transpose

# Attributes
fieldseparator = ',' # Fields separator
datafileoutpath = "../data/synthetic/synthetic-simple" # Data file
classfieldnum = 0 # class (Y) field position
fields = 4 # Fields amount
fieldstotal = 20 # Fields amount
rowscount = 20000 # Rows amount
classes = [0,1] # Min and max of Y as real variable
noise_stddev = 0.04 # Normal noise

# Gram-Schmidt by iizukak (https://gist.github.com/1287876)
def gsCofficient(v1, v2):
    return dot(v2, v1) / dot(v1, v1)

def multiply(cofficient, v):
    return map((lambda x : x * cofficient), v)

def proj(v1, v2):
    return multiply(gsCofficient(v1, v2) , v1)

def gs(X):
    Y = []
    for i in range(len(X)):
        temp_vec = X[i]
        for inY in Y :
            proj_vec = proj(inY, X[i])
            temp_vec = map(lambda x, y : x - y, temp_vec, proj_vec)
        Y.append(temp_vec)
    return Y

def doubleToStr(x):
	if type(x) is not str:
		return "%.9f" % x
	else:
		return x

# Algo
# Open output file
fout = open(datafileoutpath, 'w+')
# Make orthonormal vectors
H = random.rand(fields - 1, fieldstotal - 1)
H = array(gs(H))
H = map(lambda v: map(lambda c: c / linalg.norm(v), v), H)
H = transpose(H)
# Generate hyperplain cofficients for Y
hyperplain = random.randn(fields - 1)
for line in xrange(0, rowscount):
	x = random.randn(fields - 1) # Generate current x
	curclass = dot(hyperplain, x) # Generate y as x response from hyperplain
	xout = map(lambda c: dot(x, c), H) # x in space with more size
	xout = xout + random.normal(0, noise_stddev, fieldstotal - 1) # Adding noise to x response
	lineout = doubleToStr(curclass) + ',' + reduce(lambda x, y: doubleToStr(x) + ',' + doubleToStr(y), xout) + '\n'
	fout.write(lineout)
fout.close()