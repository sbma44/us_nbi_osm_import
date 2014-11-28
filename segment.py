import sys
import csv
import us

def main():
    state_writers = {}
    for s in us.states.STATES:
        state_writers[s.fips] = csv.writer(open('nbi/nbi_{state}.csv'.format(state=s.abbr), 'w'))

    with open(sys.argv[1], 'r') as f:
        i = 0
        reader = csv.reader(f)
        
        # discard first row
        next(reader)
    
        while True:
            try:
                row = next(reader)
            except StopIteration:
                print('Done, processed {i} rows'.format(i=i))
                break
            except:
                print('Error at row {i}, continuing...'.format(i=i))
                print(sys.exc_info()[0])
                x = input()                
                continue

            key = row[0].strip()[:2]
            if (len(key) == 0) or not (key in state_writers):
                print('Missing or bad FIPS code ({fips}), continuing...'.format(fips=key))
                continue

            state_writers[row[0][:2]].writerow(row)
            i = i + 1
            if (i % 10000) == 0:
                print('Processed {i} records'.format(i=i))

if __name__ == '__main__':
    main()