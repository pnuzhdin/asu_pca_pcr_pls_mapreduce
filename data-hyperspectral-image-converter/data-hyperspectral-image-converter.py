# Altai State University, Pavel Nuzhdin, 2012
import data_handler

# Attributes
datafilepath = "../../data/hyperspectral-image/09174_hslvl3_swir-i_hx06.bsq"
numlines = 4113
pixperline = 831 # Samples
numbands = 147
dataformat = "<H" # Little-endian unsigned short (2 byte)

bands = data_handler.readBsq(datafilepath, numlines, pixperline, numbands, dataformat)
fout = open("bands.out", "w+")
fout.write(bands)
fout.close()