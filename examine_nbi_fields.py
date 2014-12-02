import csv

with open('nbi/nbi.csv') as nbi_f:
    reader = csv.reader(nbi_f)
    next(reader)
    i = 0
    h = 0
    for row in reader:
        record_type = row[2].strip()
        if record_type != '1':
            if len(row[14].strip())>0:
                print(row[0],row[14])
                h = h + 1                
            i = i + 1
print (h, '/', i)