//
// Created by liqi on 19-6-22.
//

#include <iostream>
#include <cstring>
#include "pBellmanFordClient.h"
#include "../../UNIX_MacroProber.h"

pBellmanFordClient::pBellmanFordClient(int vCount, int eCount, int numOfInitV, int nodeNo, int batch)
: UtilClient(vCount, eCount, numOfInitV, nodeNo) {

    this->clientProber_msg = UNIX_msg();
    this->proberData_msg = UNIX_msg();
    this->init_msg = UNIX_msg();

    this->tempNode_shm = UNIX_shm();
    this->tempData_shm = UNIX_shm();
    this->tempActive_shm = UNIX_shm();

    int batches = batch;

    int chk = 0;

    if(chk != -1)
        chk = this->tempNode_shm.create(((this->nodeNo << NODE_NUM_OFFSET) | (TEMPNODE_SHM << SHM_OFFSET)),
                batches * sizeof(int),
                0666);
    if(chk != -1)
        chk = this->tempActive_shm.create(((this->nodeNo << NODE_NUM_OFFSET) | (TEMPACTIVE_SHM << SHM_OFFSET)),
                batches * sizeof(bool),
                0666);
    if(chk != -1)
        chk = this->tempData_shm.create(((this->nodeNo << NODE_NUM_OFFSET) | (TEMPDATA_SHM << SHM_OFFSET)),
                (batches * numOfInitV) * sizeof(double),
                0666);

    if(chk != -1)
        chk = this->init_msg.create(((this->nodeNo << NODE_NUM_OFFSET) | (INIT_MSG_TYPE << MSG_TYPE_OFFSET)),
                                              0666);
    if(chk != -1)
        chk = this->clientProber_msg.create(((this->nodeNo << NODE_NUM_OFFSET) | (PROCLI_MSG_TYPE << MSG_TYPE_OFFSET)),
                                              0666);
    if(chk != -1)
        chk = this->proberData_msg.create(((this->nodeNo << NODE_NUM_OFFSET) | (PRODT_MSG_TYPE << MSG_TYPE_OFFSET)),
                                              0666);

    if(chk != -1) {

        this->tempNode_shm.attach(0666);
        this->tempActive_shm.attach(0666);
        this->tempData_shm.attach(0666);

        this->tempNode = (int *) this->tempNode_shm.shmaddr;
        this->tempActive = (bool *) this->tempActive_shm.shmaddr;
        this->tempData = (double *) this->tempData_shm.shmaddr;
    }

    this->vSet = nullptr;

    this->init_msg.send("initiated client", (INIT_MSG_TYPE << MSG_TYPE_OFFSET), 256);
}

int pBellmanFordClient::connectToServer() {
    return this->connect();
}

void pBellmanFordClient::execute() {

    int dataUnderCount = 0;

    long* VSetNode = new long[this->vCount];
    bool* VSetActive = new bool[this->vCount];

    double* VSetValues = new double[this->vCount * numOfInitV];

    char msgp[256];
    std::string cmd = std::string("");

    int iterCount = 0;

    while(this->proberData_msg.recv(msgp, (PRODT_MSG_TYPE << MSG_TYPE_OFFSET), 256) != -1 &&
          dataUnderCount <= this->vCount){

        //Test
        std::cout << "Processing at iter " << ++iterCount << std::endl;
        //Test end

        cmd = msgp;
        int vertexBatch = atoi(msgp);

        if(vertexBatch > 0){

            memcpy(&VSetValues[dataUnderCount * numOfInitV],
                    this->tempData, vertexBatch * numOfInitV * sizeof(double));

            memcpy(&VSetNode[dataUnderCount * numOfInitV],
                   this->tempNode, vertexBatch * sizeof(long));
            memcpy(&VSetActive[dataUnderCount * numOfInitV],
                   this->tempActive, vertexBatch * sizeof(bool));

            dataUnderCount += vertexBatch;

            this->clientProber_msg.send("finished", (PROCLI_MSG_TYPE << MSG_TYPE_OFFSET), 256);
        }
        else if(std::string("end") == cmd)
            break;
        else if(std::string("terminate") == cmd){
            exit(0);
        }
        else break;
    }

    //convert array to vertex sets

    std::vector<Vertex> vertices;

    for(int i = 0; i < this->vCount; i++){
        vertices.emplace_back(Vertex(i, false, INVALID_INITV_INDEX));
        for(int j = 0; j < numOfInitV; j++){
            VSetValues[i * numOfInitV + j] = (INT32_MAX >> 1);
        }
    }

    for(int i = 0; i < numOfInitV; i++){
        vertices.at(this->initVSet[i]).initVIndex = i;
    }

    for (int i = 0; i < dataUnderCount; i++) {

        long jVertexId_get = VSetNode[i];

        for(int j = 0; j < numOfInitV; j++){
            double jDis = VSetValues[i * numOfInitV + j] ;
            int index = vertices.at(this->initVSet[j]).initVIndex;
            if(index != INVALID_INITV_INDEX)vValues[jVertexId_get * numOfInitV + index] = jDis;
        }

        bool jVertexActive_get = VSetActive[i];

        vertices.at(jVertexId_get).isActive = jVertexActive_get;

    }

    int sts = this->update(VSetValues, &vertices[0]);

    //Test
    std::cout << "Batch copy complete in " << sts << std::endl;
    //Test end

    this->request();

    this->clientProber_msg.send("calculation completed", (PROCLI_MSG_TYPE << MSG_TYPE_OFFSET), 256);
}