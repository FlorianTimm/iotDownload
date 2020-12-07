from io import StringIO
import os
import requests

urlPrefix = 'https://iot.hamburg.de/v1.1/'

sensorDataStream = 'Sensors(1)/Datastreams' #Stadtrad

pfad = "stadtrad/"

proxies = {
  'http': 'http://wall.lit.hamburg.de:80',
  'https': 'http://wall.lit.hamburg.de:80',
}

def loadJson (url):
    r = requests.get(url, proxies = proxies)
    return r.json()

def multiPage(url):
    value = []
    while True:
        json = loadJson(url)
        if (len(json['value']) == 0):
            break
        value = value + json['value']

        if not '@iot.nextLink' in json:
            break
        url = json['@iot.nextLink']
    return value


sensors = multiPage(urlPrefix + sensorDataStream)

print('Anzahl Sensoren: ' + str(len(sensors)))
print(sensors[0])

def filename (fn):
    fn = fn.replace(' ','')
    fn = fn.replace('-','')
    fn = fn.replace('ä','ae')
    fn = fn.replace('ö','oe')
    fn = fn.replace('ü','ue')
    fn = fn.replace('ä','ae')
    fn = fn.replace('ö','oe')
    fn = fn.replace('ü','ue')
    fn = fn.replace('/','_')
    return fn

for sensor in sensors:

    thing = loadJson(sensor['Thing@iot.navigationLink'])
    descr = thing['description']
    print(descr)
    dateipfad = pfad + filename(descr) + '.csv';
    if os.path.exists(pfad + filename(descr) + '.csv'):
        continue
    csv = open(dateipfad,'w+')
    csv.write('beschreib; time; result\n')
    obs = multiPage(sensor['Observations@iot.navigationLink'])

    for observation in obs:
        result =  observation['result']
        time = observation['phenomenonTime']
        csv.write('"' + str(descr) + '";"' + str(time) + '";' + str(result) + '\n')

    csv.close()