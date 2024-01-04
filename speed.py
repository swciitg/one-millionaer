import sys
speed = int(sys.argv[1]) # files per second
total = 5000000
time = (total / speed) / (60*60*24)
print(time, 'days')
