import sys

def get(strPropertyFilePath):
    with open(strPropertyFilePath, 'r') as f:
        propertyDict = {}
        for line in f:
            #remove whitespace and \n chars
            line = line.rstrip()
            
            #skips blands and comments w/ '='
            if "=" not in line:
                continue

            if line.startswith("#"):
                continue

            k, v = line.split("=", 1)
            propertyDict[k] = v
        return propertyDict

