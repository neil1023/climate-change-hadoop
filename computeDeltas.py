import csv
import operator

cities = {}
with open('output_results') as f:
    for line in f:
        l = line.replace('\t', '').replace('\n', '').split(',', 4)
        if l[0] in cities and ('1950' in l[1] or '2016' in l[1]):
            if l[1] in cities[l[0]]:
                cities[l[0]][l[1]][l[2]] = l[3]
            else:
                cities[l[0]][l[1]] = {}
                cities[l[0]][l[1]][l[2]] = l[3]
        elif '1950' in l[1] or '2016' in l[1]:
            cities[l[0]] = {}
            cities[l[0]][l[1]] = {}
            cities[l[0]][l[1]][l[2]] = l[3]

    f = open('deltas.csv', 'w+')
    for key in cities:
        value = cities[key]
        for date in value:
            dl = date.split('-', 3)
            year = dl[0]
            month = dl[1]
            day = dl[2]
            
            delta_tasmax = 0
            delta_tasmin = 0
            if (year == '1950'):
                future_date = '2016-' + month + '-' + day
                delta_tasmax = float(value[future_date]['tasmax']) - float(value[date]['tasmax'])
                delta_tasmin = float(value[future_date]['tasmin']) - float(value[date]['tasmin'])
            else:
                continue
            
            f.write('%s,%s,delta_tasmax,%s\n' % (key, date, delta_tasmax))
            f.write('%s,%s,delta_tasmin,%s\n' % (key, date, delta_tasmin))
   
    f.close()
    f2 = open('deltas.csv', 'r') 
    deltas_reader = csv.reader(f2, delimiter=',')
    srt = sorted(deltas_reader, key=lambda x: abs(float(x[3])), reverse=True)
    
    srt_deltas = open('sorted_deltas.csv', 'w+')
    csv_writer = csv.writer(srt_deltas)
    csv_writer.writerows(srt)

    f.close()
    srt_deltas.close() 
    
