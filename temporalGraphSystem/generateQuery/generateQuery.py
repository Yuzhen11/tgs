from random import randint
import sys
file = ['edit', 'delicious', 'wiki', 'flickr', 'youtube', 'dblp']
vertexNum = [21504191,4535197,1870709,2302925,3223589,1103412]
file = ['edit']
for i in range(len(file)):
    print file[i]
    f = open(file[i]+'.earliest.query', 'w')
    for j in range(int(sys.argv[1])):
        line = str(6) + " " + str(randint(0, vertexNum[i]-1)) + '\n'
        f.write(line)
    f.close()
