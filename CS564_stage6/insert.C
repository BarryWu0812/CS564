#include "catalog.h"
#include "query.h"

const Status QU_Insert(const string & relation,
                       const int attrCnt,
                       const attrInfo attrList[])
{
    cout << "Doing QU_Insert..." << endl;

    Status status;

    // Step 1: Validate relation
    RelDesc relDesc;
    status = relCat->getInfo(relation, relDesc);
    if (status != OK) {
        cerr << "Relation does not exist: " << relation << endl;
        return status;
    }

    // Step 2: Validate attributes
    AttrDesc *attrs;
    int numAttrs;
    status = attrCat->getRelInfo(relation, numAttrs, attrs);
    if (status != OK || attrCnt != numAttrs) {
        cerr << "Attribute mismatch for relation: " << relation << endl;
        return ATTRNOTFOUND;
    }

    // 計算記錄長度
    int recordLength = 0;
    for (int i = 0; i < numAttrs; i++) {
        recordLength += attrs[i].attrLen;
    }

    // 為記錄分配內存
    char *recordData = new char[recordLength];
    memset(recordData, 0, recordLength);

    // Step 3: Prepare record
    for (int i = 0; i < attrCnt; i++) {
        for (int j = 0; j < numAttrs; j++) {
            if (strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {
                void *value = attrList[i].attrValue;
                if (attrList[i].attrType == INTEGER) {
                    int intValue = atoi(static_cast<const char*>(attrList[i].attrValue)); // Convert string to integer
                    memcpy(recordData + attrs[j].attrOffset, &intValue, sizeof(int));
                } else if (attrList[i].attrType == FLOAT) {
                    float floatValue = atof(static_cast<const char*>(attrList[i].attrValue)); // Convert string to float
                    memcpy(recordData + attrs[j].attrOffset, &floatValue, sizeof(float));
                }
                else if (attrs[j].attrType == STRING) {
                    strncpy(recordData + attrs[j].attrOffset, static_cast<const char*>(attrList[i].attrValue), attrs[j].attrLen);
                } else {
                    cerr << "Unsupported attribute type for attribute: " << attrList[i].attrName << endl;
                    delete[] recordData;
                    delete[] attrs;
                    return ATTRNOTFOUND;
                }
                break;
            }
        }
    }

    // Step 4: Insert record
    InsertFileScan insertFile(relation, status);
    if (status != OK) {
        delete[] recordData;
        delete[] attrs;
        return status;
    }

    RID rid;
    Record rec = {recordData, recordLength};
    status = insertFile.insertRecord(rec, rid);

    // Cleanup
    delete[] recordData;
    delete[] attrs;

    return status;
}
