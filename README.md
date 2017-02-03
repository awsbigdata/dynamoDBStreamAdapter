# dynamoDBStreamAdapter

 a) steps call DynamoDBStreamAdapter from jython.
 
 Prerequisites 
     - Install jython 
     - Install gradle 
     - Install >= Java 1.7 
     - Clone the code to your machine
     - Update table and region name in the script
     
Execute below command to create a fatJar :

gradle clean build fatjar

go to build/libs directory and you will see kinesis-all-1.0-SNAPSHOT.jar

export CLASSPATH=$CLASSPATH:/pathtojar/kinesis-all-1.0-SNAPSHOT.jar

jython dynamoStreamReader.py     
     
Code :
======================

# snipped to call java from jython 

from com.amazonaws.codesamples.gsg import StreamsRecordProcessorFactory
from com.amazonaws.codesamples.gsg import StreamsAdapterDemo
from com.amazonaws.codesamples.gsg import StreamsRecordProcessor

class Jprocessor(StreamsRecordProcessor):
    def __init__(self):
        pass
        
# this method will be called to process the records 

 def processRecords(self,records,checkpointer):
 	for record in records:
		print("from python",self.getRecord(record))

if  __name__  ==  "__main__": 

	table='hivedb'
	endpoint='https://dynamodb.us-east-1.amazonaws.com'
        jp=Jprocessor()
	sp=StreamsRecordProcessorFactory()
	sp.setProcessor(jp)
	sa=StreamsAdapterDemo()
	sa.jthonCall(sp,table,endpoint)
        
=============	

To print in console:

java -jar kinesis-all-1.0-SNAPSHOT.jar hivedb https://dynamodb.us-east-1.amazonaws.com

java -jar kinesis-all-1.0-SNAPSHOT.jar {tablename} {endpoint}










