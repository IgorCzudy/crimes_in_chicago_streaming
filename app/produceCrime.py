from confluent_kafka import Producer
from datetime import datetime
import csv
from CrimeRecord import CrimeRecord

def deliver_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def read_crime_record(number_of_line: int) -> CrimeRecord:
    with open('app/sorted_crimes-2022.csv', mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for current_line, row in enumerate(reader, start=1):
            if current_line == number_of_line:


                row['ID'] = int(row['ID'])
                row['Date'] = datetime.strptime(row['Date'], "%m/%d/%Y %I:%M:%S %p") #07/08/2022 10:38:00 AM
                row['Arrest'] = row['Arrest'].lower() == 'true'
                row['Domestic'] = row['Domestic'].lower() == 'true'
                row['Beat'] = int(row['Beat'])
                row['District'] = int(row['District']) if row['District'] else None
                row['Ward'] = int(row['Ward']) if row['Ward'] else None
                row['Community_Area'] = int(row['Community_Area']) if row['Community_Area'] else None
                row['X_Coordinate'] = int(row['X_Coordinate']) if row['X_Coordinate'] else None
                row['Y_Coordinate'] = int(row['Y_Coordinate']) if row['Y_Coordinate'] else None
                row['Year'] = int(row['Year'])
                row['Updated_On'] = datetime.strptime(row['Updated_On'], "%m/%d/%Y %I:%M:%S %p") #11/12/2022 03:46:21 PM
                row['Latitude'] = float(row['Latitude']) if row['Latitude'] else None
                row['Longitude'] = float(row['Longitude']) if row['Longitude'] else None
                
                print(row)
                return CrimeRecord(**row)


def main():
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    
    i=0
    while True:
        i+=1
        crimeRecord = read_crime_record(i)
        crimeRecord_json = crimeRecord.model_dump_json()

        p.produce('crime',
            key =str(i).encode('utf-8'),
            value = crimeRecord_json.encode('utf-8'),
            callback=deliver_report)

        # time.sleep(0.6)
        p.poll(0)

    p.flush()            


        
if __name__=="__main__":
    main()