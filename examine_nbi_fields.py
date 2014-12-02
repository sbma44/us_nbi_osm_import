import csv

with open('nbi/nbi.csv') as nbi_f:
    reader = csv.reader(nbi_f)
    next(reader)
    i = 0
    h = 0
    for row in reader:
        record_type = row[2].strip()
        merged_clearance = row[14]
        granular_clearance = 0
        other_clearance = 0
        if record_type == '1':
            granular_clearance = row[60]   
            other_clearance = row[62]
        else:
            granular_clearance = row[62]            
            other_clearance = row[60]

        if record_type=='1' and merged_clearance!=granular_clearance:
            h = h + 1
            # print(float(merged_clearance) - float(granular_clearance))
            print(record_type, merged_clearance, granular_clearance, other_clearance)
        i = i + 1
print (h, i)