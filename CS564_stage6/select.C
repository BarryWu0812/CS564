#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"
#include "heapfile.h"  // To use HeapFileScan
#include "utility.h"   // For helper functions

// forward declaration
const Status ScanSelect(const string & result,
            const int projCnt,
            const AttrDesc projNames[],
            const AttrDesc *attrDesc,
            const Operator op,
            const char *filter,
            const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 *     OK on success
 *     an error code otherwise
 */
const Status QU_Select(const string & result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
    cout << "Doing QU_Select..." << endl;

    Status status;

    // Retrieve projection attributes from catalog
    AttrDesc projAttrs[projCnt];
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projAttrs[i]);
        if (status != OK) {
            cerr << "Error retrieving projection attribute: " << projNames[i].attrName << endl;
            return status;
        }
    }

    // Retrieve filter attribute descriptor
    AttrDesc filterAttr;
    if (attr != nullptr) {
        status = attrCat->getInfo(attr->relName, attr->attrName, filterAttr);
        if (status != OK) {
            cerr << "Error retrieving filter attribute: " << attr->attrName << endl;
            return status;
        }
    }

    // Compute record length
    int reclen = 0;
    for (int i = 0; i < projCnt; i++) {
        reclen += projAttrs[i].attrLen;
    }

    // Execute scan and selection
    return ScanSelect(result, projCnt, projAttrs, attr ? &filterAttr : nullptr, op, attrValue, reclen);
}

// ScanSelect: Executes a scan, applies filters, and inserts results into the target relation
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projAttrs[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filterValue,
                        const int reclen)
{
    cout << "Executing ScanSelect..." << endl;

    Status status;

    // Initialize HeapFileScan
    HeapFileScan scan(attrDesc ? attrDesc->relName : projAttrs[0].relName, status);
    if (status != OK) return status;


    // Convert `filter` for numeric attributes if needed
    const char *convertedFilter = filterValue;
    char buffer[sizeof(float)]; // Buffer to hold binary representation

    if (attrDesc != nullptr) {
        if (attrDesc->attrType == INTEGER) {
            int intValue = atoi(filterValue); // Convert string to integer
            memcpy(buffer, &intValue, sizeof(int));
            convertedFilter = buffer; // Use binary representation
        } else if (attrDesc->attrType == FLOAT) {
            float floatValue = atof(filterValue); // Convert string to float
            memcpy(buffer, &floatValue, sizeof(float));
            convertedFilter = buffer; // Use binary representation
        }
    }

    // Apply filter if a filter attribute is provided
    if (attrDesc) {
        status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), convertedFilter, op);
        if (status != OK) return status;
    } else {
        status = scan.startScan(0, 0, STRING, nullptr, EQ);
        if (status != OK) return status;
    }

    // Prepare target relation
    InsertFileScan insertFile(result, status);
    if (status != OK) {
        cerr << "Error initializing InsertFileScan" << endl;
        return status;
    }
    if (status != OK) return status;

    // Scan and process matching records
    RID rid;
    while (scan.scanNext(rid) == OK) {
        Record rec;
        scan.getRecord(rec);

        // Project attributes into a new record
        char *newData = new char[reclen];
        int offset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(newData + offset, static_cast<char*>(rec.data) + projAttrs[i].attrOffset, projAttrs[i].attrLen);
            offset += projAttrs[i].attrLen;
        }

        Record newRec = {newData, reclen};
        status = insertFile.insertRecord(newRec, rid);
        if (status != OK) {
            delete[] newData;
            return status;
        }

        delete[] newData;
    }

    scan.endScan();
    return OK;
}