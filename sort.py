fin = open("celltpoints.txt", "r")
fout = open("celltpoints_sorted.txt", "w")

nameslist = []

line = fin.readline()
while line:
    nameslist.append(line)
    line = fin.readline()

nameslist.sort()

for line in nameslist:
    fout.write(line)
    
fin.close()
fout.close()
