from datetime import datetime
from novalabs import log
import payment

class amazon:

    _AWSAccessKey = None
    _AWSSecretKey = None

    _payments = None
    _startDate = None
    _endDate = None

    def __init__(self, accessKey, secretKey):
	    
        self._AWSAccessKey = accessKey
        self._AWSSecretKey = secretKey

    def getAccessKey(self):
        return self._AWSAccessKey

    def getSecretKey(self):
        return self._AWSSecretKey

    def getStartDate(self):
        return self._startDate

    def getEndDate(self):
        return self._endDate

    def setStartDate(self, startDate):
        if type(startDate) is datetime:
            self._startDate = startDate
        else:
            try:
                self.startDate = datetime.strptime(startDate, '%Y-%m-%d')
            except TypeError:
                log.writeError('StartDate parameter cannot be cast to a datetime object') 


    def setEndDate(self, endDate):
        if type(endDate) is datetime:
            self._endDate = endDate
        else:
            try:
                self._endDate = datetime.strptime(endDate, '%Y-%m-%d')
            except TypeError:
                log.writeError('EndDate parameter cannot be cast to a datetime object')

    def getTransactionCount(self):
        if self._payments == None:
            return 0
        else:
            return len(self._payments)

    def getPaymentResults(self):
        return self._payments

    def getPayments(self, startDate, endDate):

        # retrieve payment data from Amazon via api
        try:
            self.setStartDate(startDate)
            self.setEndDate(endDate)
            self._payments = payment.get(self)
        except Exception, e:
            log.writeError(e)

    def writePayments(self):

        # write payments to csv file
        try:
            payment.write(self)
        except:
            log.writeError(e)


    def loadPayments(self):
        try:
            payment.load(self)
        except Exception, e:
            log.writeError(e)
