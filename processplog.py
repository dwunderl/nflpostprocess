# -*- coding: utf-8 -*-
import csv 
import sys

from operator import itemgetter, attrgetter

#################
# Define classes for a single partial schedule
# Define class for an aggregated partial schedule set sharing the same fingerprint
#   Not currently used - the aggregated class - but might be used - and sorted differently
#################

class PartialLogEntry:

    def __init__(self, fingerPrint, weekNum, iteration, unscheduled, baseFingerPrint):
        self.fingerPrint = float(fingerPrint)
        self.weekNum = int(weekNum)
        self.iteration = int(iteration)
        self.unscheduled = int(unscheduled)
        self.baseFingerPrint = float(baseFingerPrint)

class PartialLogAggregateEntry:

    def __init__(self, fingerPrint, count, weekNum, iterationMin, iterationMax, unscheduled, baseFingerPrint):
        self.fingerPrint = float(fingerPrint)
        self.count = int(count)
        self.weekNum = float(weekNum)
        self.iterationMin = int(iterationMin)
        self.iterationMax = int(iterationMax)
        self.unscheduled = float(unscheduled)
        self.baseFingerPrint = float(baseFingerPrint)
        self.firstWeek = True

###############
# Function: Read raw partial schedule log entries from csv file
###############
def readPlog(plog) :
    f = open("logPartialScheduleResults.csv", "rt")
    reader = csv.reader(f)
    for row in reader:
        if row[0].lower() != "fingerprint" :
            ple = PartialLogEntry(float(row[0]),int(row[1]),int(row[2]),int(row[3]),float(row[4]))
            plog.append(ple)        
            #print str(ple.fingerPrint) + ", " + str(ple.weekNum) + ", " + str(ple.iteration) + ", " + str(ple.unscheduled) + ", " + str(ple.baseFingerPrint) + ", " + str(2.0*ple.fingerPrint)
 
    f.close()

###############
# Function: Write sorted partial log file
#           Records have been sorted by FingerPrint
###############
def writeSortedPleFile(sPlogList) :
    ofile  = open('partialSorted.csv', "wt")
    writer = csv.writer(ofile,lineterminator="\n")
     
    rowtup = ("FingerPrint","Week", "Iteration", "Unscheduled", "BaseFP")
    writer.writerow(rowtup)

    for ple in sPlogList:
        rowtup = (ple.fingerPrint,ple.weekNum,ple.iteration,ple.unscheduled,ple.baseFingerPrint)
        writer.writerow(rowtup)

    ofile.close()

###############
# Function: Write aggregated partial log file
#           Aggregated by FingerPrint of the partial schedule
#           With a count field added for the aggregation count
#           Records have been sorted by FingerPrint
###############
def writeAggPleFile(sPlogList, pLogAggDict) :
    ofile  = open('partialAggregated.csv', "wt")
    plogAggWriter = csv.writer(ofile,lineterminator="\n")

    plogagg = []
    prevFP = -1.0
    countFP = 0
    weekSum = 0
    unscheduledSum = 0
    baseFP = 0
    iterationMin = 0
    iterationMax = 0
    plogAggList = []   # partial schedule log aggregate entry list

    rowtup = ("FingerPrint","Count", "Week", "IterationMin", "IterationMax", "Unscheduled", "BaseFP")
    plogAggWriter.writerow(rowtup)

    for ple in sPlogList:
        if ple.fingerPrint != prevFP:
            if prevFP >= 0.0:
                # write out the aggregated line
                weekAvg = float(weekSum) / float(countFP)
                unscheduledAvg = float(unscheduledSum) / float(countFP)
                plogAggEntry = PartialLogAggregateEntry(prevFP,countFP, weekAvg, iterationMin, iterationMax, unscheduledAvg, baseFP)
                writeAggRecord(plogAggEntry, plogAggList, pLogAggDict, plogAggWriter)

            countFP = 1
            prevFP = ple.fingerPrint
            weekSum = ple.weekNum
            iterationMin = ple.iteration
            iterationMax = ple.iteration
            unscheduledSum = ple.unscheduled
            baseFP = ple.baseFingerPrint
            
        else:
            countFP += 1
            weekSum += ple.weekNum
            unscheduledSum += ple.unscheduled
            if ple.iteration < iterationMin:
                iterationMin = ple.iteration
            elif ple.iteration > iterationMax:
                iterationMax = ple.iteration
            if ple.baseFingerPrint != baseFP:
                baseFP = -1.0
            
    # write out the final aggregated line
    weekAvg = float(weekSum) / float(countFP)
    unscheduledAvg = float(unscheduledSum) / float(countFP)
    plogAggEntry = PartialLogAggregateEntry(prevFP,countFP, weekAvg, iterationMin, iterationMax, unscheduledAvg, baseFP)
    writeAggRecord(plogAggEntry, plogAggList, pLogAggDict, plogAggWriter)

    ofile.close()
    
###############
# Function: Write Aggregated record to partial log aggregation file
#           And put into plogAggDict
###############
def writeAggRecord(plogAggEntry, plogAggList, plogAggDict, plogAggWriter) :
    plogAggList.append(plogAggEntry)
    plogAggDict[plogAggEntry.fingerPrint] = plogAggEntry
    rowtuple = (plogAggEntry.fingerPrint, plogAggEntry.count, plogAggEntry.weekNum,
                plogAggEntry.iterationMin, plogAggEntry.iterationMax,
                plogAggEntry.unscheduled, plogAggEntry.baseFingerPrint)
    plogAggWriter.writerow(rowtuple)

###############
# Function: Write Schedule Sequen e record to csv file
###############
def writeSchedSequenceRecord(plogAggEntry,  plogSSWriter) :
    rowtuple = (plogAggEntry.fingerPrint, plogAggEntry.count, plogAggEntry.weekNum,
                plogAggEntry.iterationMin, plogAggEntry.iterationMax,
                plogAggEntry.unscheduled, plogAggEntry.baseFingerPrint, plogAggEntry.firstWeek)
    plogSSWriter.writerow(rowtuple)

###############
# Function: Write Scheduled Sequence of weeks for Aggregated record to csv file
#           Start with a completed schedule that completes week 1 - if present
###############
def writeAggScheduleSequences(pLogAggDict) :
    ofile  = open('partialSchedSequences.csv', "wt")
    plogAggWriter = csv.writer(ofile,lineterminator="\n")

    rowtup = ("FingerPrint","Count", "Week", "IterationMin", "IterationMax", "Unscheduled", "BaseFP", "FirstWeek")
    plogAggWriter.writerow(rowtup)
    print(" pLogAggDict len is " + str(len(pLogAggDict)))
    s1 = sorted(pLogAggDict.values(), key=attrgetter('unscheduled'))
    s2 = sorted(s1, key=attrgetter('weekNum'))
    print(" s2 len is " + str(len(s2)))

    for pleAgg in s2 :
        if (pleAgg.unscheduled == 0 and pleAgg.weekNum == 1) or (pleAgg.unscheduled > 0 and pleAgg.weekNum > 1) :
        #if (pleAgg.unscheduled == 0 and pleAgg.weekNum == 1) :
            pleAgg.firstWeek = True
            writeSchedSequenceRecord(pleAgg, plogAggWriter)

            weekNum = pleAgg.weekNum
            baseFingerPrint = pleAgg.baseFingerPrint
            while weekNum < 17 :
                pleAggParent = pLogAggDict.get(baseFingerPrint)
                pleAggParent.firstWeek = False
                writeSchedSequenceRecord(pleAggParent, plogAggWriter)
                baseFingerPrint = pleAggParent.baseFingerPrint
                weekNum = pleAggParent.weekNum

            
    ofile.close()

#################
# Main Program Start
#################

print("Hello, World!")

#################
# Read raw partial log results csv file
#################

plog = [] # Partial Schedule Log Entry List
readPlog(plog)

print ("plog size is " + str(len(plog)))

#################
# Sort partial log results
# Primary Key: fingerPrint ascending
# Secondary Key: weekNum descending
# Tertiary Key: iteration ascending
#################

s1 = sorted(plog, key=attrgetter('iteration'))
s2 = sorted(s1, key=attrgetter('weekNum'),reverse=True)
s3 = sorted(s2, key=attrgetter('fingerPrint'))

#################
# Write sorted partial log results csv file
#################
writeSortedPleFile(s3)

#################
# Aggregate scheduled weeks with the same fingerprint
# Generate a count column for the number of times the same partial schedule was generated
# Use the sorted records of partial schedule results - which were sorted by fingerprints
# Write out the aggregated records as each new fingerprint is encountered
#################

pLogAggDict = {}
writeAggPleFile(s3, pLogAggDict)
writeAggScheduleSequences(pLogAggDict)


