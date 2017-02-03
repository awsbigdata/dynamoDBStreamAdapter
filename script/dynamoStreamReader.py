from com.amazonaws.codesamples.gsg import StreamsRecordProcessorFactory
from com.amazonaws.codesamples.gsg import StreamsAdapterDemo
from com.amazonaws.codesamples.gsg import StreamsRecordProcessor

class Jprocessor(StreamsRecordProcessor):
    def __init__(self):
        pass

    def processRecords(self,records,checkpointer):
 	for record in records:
		print("from python",self.getRecord(record))


if __name__ == "__main__": 
	table='hivedb'
	endpoint='https://dynamodb.us-east-1.amazonaws.com'
        jp=Jprocessor()
	sp=StreamsRecordProcessorFactory()
	sp.setProcessor(jp)
	sa=StreamsAdapterDemo()
	sa.jthonCall(sp,table,endpoint)
	


