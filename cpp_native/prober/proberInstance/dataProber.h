//
// Created by liqi on 19-6-22.
//

#ifndef PREGELGPU_DATAPROBER_H
#define PREGELGPU_DATAPROBER_H


#include "srv/UNIX_msg.h"
#include "srv/UNIX_shm.h"
#include "../UNIX_MacroProber.h"


class dataProber {
public:
    dataProber(int pid, int vertexCount, int attrCount);
    int connectToClient();
    int sendToClient(long* vertexNode, bool* vertexActive, double* vertexAttr);
    int resultGenerate();
    void disconnect();

private:
    int nodeNo;
    int vertexCount;
    int attrCount;

    int* tempNode;
    bool* tempActive;
    double* tempData;

    UNIX_shm tempNode_shm;
    UNIX_shm tempActive_shm;
    UNIX_shm tempData_shm;

    UNIX_msg proberData_msg;
    UNIX_msg clientProber_msg;

    UNIX_msg init_msg;
};


#endif //PREGELGPU_DATAPROBER_H
