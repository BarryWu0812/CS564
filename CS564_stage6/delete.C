#include "catalog.h"
#include "query.h"
#include "heapfile.h"
#include "stdlib.h"

const Status QU_Delete(const string & relation,
                       const string & attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue) {
    cout << "Executing QU_Delete..." << endl;

    Status status;

    // Step 1: Retrieve attribute descriptor
    AttrDesc attrDesc;
    if (!attrName.empty()) {
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK) {
            cerr << "Error retrieving attribute descriptor for: " << attrName << endl;
            return status;
        }
    }

    // Step 2: Convert filter value to binary format if necessary
    const char *convertedFilter = attrValue;
    char buffer[sizeof(float)];
    if (!attrName.empty()) {
        if (type == INTEGER) {
            int intValue = atoi(attrValue);
            memcpy(buffer, &intValue, sizeof(int));
            convertedFilter = buffer;
        } else if (type == FLOAT) {
            float floatValue = atof(attrValue);
            memcpy(buffer, &floatValue, sizeof(float));
            convertedFilter = buffer;
        }
    }

    // Step 3: Initialize HeapFileScan
    HeapFileScan hfs(relation, status);
    if (status != OK) {
        cerr << "Error opening HeapFileScan for relation: " << relation << endl;
        return status;
    }

    // Step 4: Start scan
    if (!attrName.empty()) {
        status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, convertedFilter, op);
        if (status != OK) {
            cerr << "Error starting scan with filter for attribute: " << attrName << endl;
            return status;
        }
    } else {
        status = hfs.startScan(0, 0, STRING, nullptr, EQ);
        if (status != OK) {
            cerr << "Error starting unfiltered scan" << endl;
            return status;
        }
    }

    // Step 5: Delete records
    RID rid;
    while (hfs.scanNext(rid) == OK) {
        status = hfs.deleteRecord();
        if (status != OK) {
            cerr << "Error deleting record" << endl;
            return status;
        }
    }

    hfs.endScan();
    return OK;
}